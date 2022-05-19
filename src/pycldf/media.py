"""
Accessing media associated with a CLDF dataset.

You can iterate over the `File` objects associated with media using the `Media` wrapper:

.. code-block:: python

    from pycldf.media import Media

    for f in Media(dataset):
        if f.mimetype.type == 'audio':
            f.save(directory)

or instantiate a `File` from a `pycldf.orm.Object`:

.. code-block:: python

    from pycldf.media import File

    f = File.from_dataset(dataset, dataset.get_object('MediaTable', 'theid'))

"""
import base64
import typing
import pathlib
import functools
import mimetypes
import urllib.parse
import urllib.request

from clldutils.misc import lazyproperty
import pycldf
from pycldf import orm
from csvw.datatypes import anyURI

__all__ = ['Mimetype', 'Media', 'File']


class File:
    """
    A File represents a row in a MediaTable, providing functionality to access the contents.

    :ivar id: The ID of the item.
    :ivar url: The URL (as `str`) to download the content associated with the item.
    """
    def __init__(self, media: 'Media', row: dict):
        self.row = row
        self.id = row[media.id_col.name]
        self._mimetype = row[media.mimetype_col.name]
        self.url = None
        self.scheme = None
        self.url_reader = media.url_reader

        if media.url_col:
            # 1. Look for a downloadUrl property:
            self.url = row[media.url_col.name]
        else:
            # 2. Expand valueUrl property:
            if media.id_col and media.id_col.valueUrl:
                self.url = media.id_col.valueUrl.expand(**row)
        if self.url:
            self.url = anyURI.to_string(self.url)
            self.parsed_url = urllib.parse.urlparse(self.url)
            self.scheme = self.parsed_url.scheme

    @classmethod
    def from_dataset(
            cls, ds: pycldf.Dataset, row_or_object: typing.Union[dict, orm.Media]) -> 'File':
        """
        Factory method to instantiate a `File` bypassing the `Media` wrapper.
        """
        return cls(
            Media(ds),
            row_or_object.data if isinstance(row_or_object, orm.Media) else row_or_object)

    def __getitem__(self, item):
        """
        Access to the underlying row `dict`.
        """
        return self.row[item]

    @lazyproperty
    def mimetype(self) -> 'Mimetype':
        """
        The `Mimetype` object associated with the item.

        While the mediaType column is required by the CLDF spec, this might be disabled.
        If so, we use "out-of-band" methods to figure out a mimetype for the file.
        """
        if self._mimetype:
            # We take the mimetype reported in the dataset as authoritative.
            return Mimetype(self._mimetype)
        # If no mimetype is specified explicitly, we fall back to mimetype detection mechanisms:
        if self.scheme in ['file', 'http', 'https']:
            mt, _ = mimetypes.guess_type(self.parsed_url.path)
            if mt:
                return Mimetype(mt)
        if self.scheme == 'data':
            mt, _, data = self.parsed_url.path.partition(',')
            if mt.endswith(';base64'):
                mt = mt.replace(';base64', '').strip()
                if mt:
                    return Mimetype(mt)
            # There's an explicit default mimetype for data URLs!
            return Mimetype('text/plain;charset=US-ASCII')
        if self.scheme in ['http', 'https']:
            res = urllib.request.urlopen(urllib.request.Request(self.url, method="HEAD"))
            mt = res.headers.get('Content-Type')
            if mt:
                return Mimetype(mt)
        return Mimetype('application/octet-stream')

    def local_path(self, d: pathlib.Path) -> pathlib.Path:
        """
        :return: The expected path of the file in the directory `d`.
        """
        return d.joinpath('{}{}'.format(self.id, self.mimetype.extension or ''))

    def read(self, d=None) -> typing.Union[None, str, bytes]:
        """
        :param d: A local directory where the file has been saved before. If `None`, the content \
        will read from the file's URL.
        """
        if d:
            return self.mimetype.read(self.local_path(d).read_bytes())
        if self.url:
            try:
                return self.url_reader[self.scheme](self.parsed_url, self.mimetype)
            except KeyError:
                raise ValueError('Unsupported URL scheme: {}'.format(self.scheme))

    def save(self, d: pathlib.Path) -> pathlib.Path:
        """
        Saves the content of `File` in directory `d`.

        :return: Path of the local file where the content has been saved.
        """
        p = self.local_path(d)
        if not p.exists():
            self.mimetype.write(self.read(), p)
        return p


class Media:
    """
    Container class for a `Dataset`'s media items.
    """
    def __init__(self, ds: pycldf.Dataset):
        self.ds = ds
        self.media_table = ds['MediaTable']
        self.url_col = ds.get(('MediaTable', 'http://cldf.clld.org/v1.0/terms.rdf#downloadUrl'))

        if not self.url_col:
            for col in self.media_table.tableSchema.columns:
                if col.propertyUrl and col.propertyUrl == 'http://www.w3.org/ns/dcat#downloadUrl':
                    self.url_col = col
                    break
        self.id_col = ds['MediaTable', 'http://cldf.clld.org/v1.0/terms.rdf#id']
        self.mimetype_col = ds['MediaTable', 'http://cldf.clld.org/v1.0/terms.rdf#mediaType']

    @lazyproperty
    def url_reader(self):
        return {
            'http': read_http_url,
            'https': read_http_url,
            'data': read_data_url,
            # file: URLs are interpreted raltive to the location of the metadata file:
            'file': functools.partial(read_file_url, self.ds.directory),
        }

    def __iter__(self) -> typing.Generator[File, None, None]:
        for row in self.media_table:
            yield File(self, row)


class Mimetype:
    """
    A media type specification.

    :ivar type: The (main) type as `str`.
    :ivar subtype: The subtype as `str`.
    :ivar encoding: The encoding specified with a "charset" parameter.
    """
    def __init__(self, s):
        self.string = s
        mtype, _, param = self.string.partition(';')
        param = param.strip()
        self.type, _, self.subtype = mtype.partition('/')
        if param.startswith('charset='):
            self.encoding = param.replace('charset=', '').strip()
        else:
            self.encoding = 'utf8'

    @property
    def is_text(self) -> bool:
        return self.type == 'text'

    @property
    def extension(self) -> typing.Union[None, str]:
        return mimetypes.guess_extension('{}/{}'.format(self.type, self.subtype))

    def read(self, data: bytes) -> typing.Union[str, bytes]:
        if self.is_text and not isinstance(data, str):
            return data.decode(self.encoding)
        return data

    def write(self, data: typing.Union[str, bytes], p: pathlib.Path) -> int:
        return p.write_bytes(data.encode(self.encoding) if self.is_text else data)


def read_data_url(url: urllib.parse.ParseResult, mimetype: Mimetype):
    spec, _, data = url.path.partition(',')
    if spec.endswith(';base64'):
        data = base64.b64decode(data)

    data = mimetype.read(data)
    if mimetype.is_text:
        data = urllib.parse.unquote(data)
    return data


def read_file_url(d: pathlib.Path, url: urllib.parse.ParseResult, mimetype: Mimetype):
    path = url.path
    while path.startswith('/'):
        path = path[1:]
    return mimetype.read(d.joinpath(path).read_bytes())


def read_http_url(url: urllib.parse.ParseResult, mimetype: Mimetype):
    return mimetype.read(urllib.request.urlopen(urllib.parse.urlunparse(url)).read())
