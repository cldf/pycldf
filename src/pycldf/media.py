"""
Accessing media associated with a CLDF dataset.

You can iterate over the :class:`.File` objects associated with media using the :class:`.Media`
wrapper:

.. code-block:: python

    from pycldf.media import Media

    for f in Media(dataset):
        if f.mimetype.type == 'audio':
            f.save(directory)

or instantiate a :class:`.File` from a :class:`pycldf.orm.Object`:

.. code-block:: python

    from pycldf.media import File

    f = File.from_dataset(dataset, dataset.get_object('MediaTable', 'theid'))

"""
import io
import json
import base64
from typing import Union, TYPE_CHECKING, Optional, Callable
import pathlib
import zipfile
import functools
import mimetypes
import collections
import urllib.parse
import urllib.request
from collections.abc import Generator

from csvw.metadata import Table, Column
from csvw.datatypes import anyURI

from pycldf import orm
from pycldf.fileutil import splitfile, catfile, PathType

if TYPE_CHECKING:
    from pycldf import Dataset  # pragma: no cover
    from pycldf.dataset import RowType  # pragma: no cover
    from pycldf.validators import DatasetValidator  # pragma: no cover

__all__ = ['Mimetype', 'MediaTable', 'File']

StrOrBytes = Union[str, bytes]


class File:  # pylint: disable=too-many-instance-attributes
    """
    A `File` represents a row in a MediaTable, providing functionality to access the contents.

    :ivar id: The ID of the item.
    :ivar url: The URL (as `str`) to download the content associated with the item.

    `File` supports media files within ZIP archives as specified in CLDF 1.2. I.e.

    - :meth:`read` will extract the specified file from a downloaded ZIP archive and
    - :meth:`save` will write a (deflated) ZIP archive containing the specified file as single \
      member.
    """
    def __init__(self, media: 'MediaTable', row: 'RowType'):
        self.row: 'RowType' = row
        self.id: str = row[media.filename_col.name]
        self._mimetype: str = row[media.mimetype_col.name]
        self.url: Optional[str] = None
        self.scheme = None
        self.url_reader = media.url_reader
        self.path_in_zip: Optional[str] \
            = row.get(media.path_in_zip_col.name) if media.path_in_zip_col else None
        self._dsdir: pathlib.Path = media.ds.directory

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
            self.scheme = self.parsed_url.scheme or 'file'
            self.relpath = self.parsed_url.path
            while self.relpath.startswith('/'):
                self.relpath = self.relpath[1:]

    @classmethod
    def from_dataset(
            cls, ds: 'Dataset', row_or_object: Union[dict, orm.Media]) -> 'File':
        """
        Factory method to instantiate a `File` bypassing the `Media` wrapper.
        """
        return cls(
            MediaTable(ds),
            row_or_object.data if isinstance(row_or_object, orm.Media) else row_or_object)

    def __getitem__(self, item) -> dict:
        """
        Access to the underlying row `dict`.
        """
        return self.row[item]

    @functools.cached_property
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
            mt, _, _ = self.parsed_url.path.partition(',')
            if mt.endswith(';base64'):
                mt = mt.replace(';base64', '').strip()
                if mt:
                    return Mimetype(mt)
            # There's an explicit default mimetype for data URLs!
            return Mimetype('text/plain;charset=US-ASCII')
        if self.scheme in ['http', 'https']:
            res = urllib.request.urlopen(  # too lazy to mock with with. pylint: disable=R1732
                urllib.request.Request(self.url, method="HEAD"))
            mt = res.headers.get('Content-Type')
            if mt:
                return Mimetype(mt)
        return Mimetype('application/octet-stream')

    def local_path(self, d: pathlib.Path = None) -> Optional[pathlib.Path]:
        """
        :return: The expected path of the file in the directory `d`.
        """
        if d is None:
            if self.scheme == 'file':
                return self._dsdir / urllib.parse.unquote(self.relpath)
            return None
        zip_ext = '.zip' if self.path_in_zip else (self.mimetype.extension or '')
        return d.joinpath(f'{self.id}{zip_ext}')

    def read_json(self, d=None):
        """Reads JSON data."""
        assert self.mimetype.subtype.endswith('json')
        return json.loads(self.read(d=d))

    def read(self, d: Optional[pathlib.Path] = None) -> Optional[StrOrBytes]:
        """
        :param d: A local directory where the file has been saved before. If `None`, the content \
        will be read from the file's URL.
        """
        if self.path_in_zip:
            zipcontent = None
            if d:
                zipcontent = self.local_path(d).read_bytes()
            if self.url:
                zipcontent = self.url_reader[self.scheme](
                    self.parsed_url, Mimetype('application/zip'))
            if zipcontent:
                with zipfile.ZipFile(io.BytesIO(zipcontent)) as zf:
                    return self.mimetype.read(zf.read(self.path_in_zip))
            return None  # pragma: no cover

        if d:
            return self.mimetype.read(self.local_path(d).read_bytes())
        if self.url:
            try:
                return self.url_reader[self.scheme](self.parsed_url, self.mimetype)
            except KeyError as e:
                raise ValueError(f'Unsupported URL scheme: {self.scheme}') from e
        return None  # pragma: no cover

    def save(self, d: pathlib.Path) -> pathlib.Path:
        """
        Saves the content of `File` in directory `d`.

        :return: Path of the local file where the content has been saved.

        .. note::

            We use the identifier of the media item (i.e. the content of the ID column of the
            associated row) as stem of the file to be written.
        """
        p = self.local_path(d)
        if not p.exists():
            if self.path_in_zip:
                with zipfile.ZipFile(p, 'w', compression=zipfile.ZIP_DEFLATED) as zf:
                    zf.writestr(self.path_in_zip, self.mimetype.write(self.read()))
            else:
                self.mimetype.write(self.read(), p)
        return p


class MediaTable:  # pylint: disable=too-many-instance-attributes
    """
    Container class for a `Dataset`'s media items.
    """
    def __init__(self, ds: 'Dataset'):
        self.ds: 'Dataset' = ds
        self.component: str = self.__class__.__name__
        self.table: Table = ds[self.component]
        self.url_col: Optional[Column] = ds.get(
            ('MediaTable', 'http://cldf.clld.org/v1.0/terms.rdf#downloadUrl'))
        self.path_in_zip_col: Optional[Column] = ds.get(
            (self.component, 'http://cldf.clld.org/v1.0/terms.rdf#pathInZip'))

        if self.table and not self.url_col:
            for col in self.table.tableSchema.columns:
                if col.propertyUrl and col.propertyUrl == 'http://www.w3.org/ns/dcat#downloadUrl':
                    self.url_col = col
                    break
        self.id_col: Column = ds[self.component, 'http://cldf.clld.org/v1.0/terms.rdf#id']
        self.filename_col: Column = self.id_col
        self.mimetype_col: Column = ds[
            self.component, 'http://cldf.clld.org/v1.0/terms.rdf#mediaType']

    @functools.cached_property
    def url_reader(self) -> dict[str, Callable[[urllib.parse.ParseResult, 'Mimetype'], StrOrBytes]]:
        """Maps URL schemes to reader functions."""
        return {
            'http': read_http_url,
            'https': read_http_url,
            'data': read_data_url,
            # file: URLs are interpreted relative to the location of the metadata file:
            'file': functools.partial(read_file_url, self.ds.directory),
        }

    def __iter__(self) -> Generator[File, None, None]:
        for row in self.table:
            yield File(self, row)

    def split(self, chunksize: int) -> int:
        """
        :return: The number of media files that needed splitting.
        """
        res = 0
        for file in self:
            p = file.local_path()
            if p and p.exists():
                size = p.stat().st_size
                if size > chunksize:
                    splitfile(p, chunksize, size)
                    res += 1
        return res

    def cat(self) -> int:
        """
        :return: The number of media files that have been re-assembled from chunks.
        """
        res = 0
        for file in self:
            p = file.local_path()
            if p and not p.exists():
                if catfile(p):
                    res += 1
        return res

    def validate(self, validator: 'DatasetValidator'):
        """Component-specific validation."""
        speaker_area_files = collections.defaultdict(list)
        if ('LanguageTable', 'speakerArea') in self.ds:
            for lg in self.ds.iter_rows('LanguageTable', 'id', 'speakerArea'):
                if lg['speakerArea']:
                    speaker_area_files[lg['speakerArea']].append(lg['id'])

        for file in self:
            self._validate_file(validator, file, speaker_area_files)

    def _validate_file(self, validator, file, speaker_area_files):
        content = None
        if not file.url:
            validator.fail(f'File without URL: {file.id}')
        elif file.scheme == 'file':
            try:
                content = file.read()
            except FileNotFoundError:
                validator.fail(
                    f'Non-existing local file referenced: {file.id} '
                    'You may have to run `cldf catmedia` to recombine files')
            except Exception as e:  # pragma: no cover  # pylint: disable=W0718
                validator.fail(f'Error reading {file.id}: {e}')
        elif file.scheme == 'data':
            try:
                content = file.read()
            except Exception as e:  # pragma: no cover  # pylint: disable=W0718
                validator.fail(f'Error reading {file.id}: {e}')
        if file.id in speaker_area_files and file.mimetype.subtype == 'geo+json' and content:
            content = json.loads(content)
            if content['type'] != 'Feature':
                assert content['type'] == 'FeatureCollection'
                for feature in content['features']:
                    lid = feature['properties'].get('cldf:languageReference')
                    if lid and lid in speaker_area_files[file.id]:
                        speaker_area_files[file.id].remove(lid)
                if speaker_area_files[file.id]:
                    validator.fail(
                        f'Error: Not all language IDs found in speakerArea GeoJSON: '
                        f'{speaker_area_files[file.id]}')  # pragma: no cover


Media = MediaTable


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

        # for compatibility reasons
        if self.type == 'audio' and self.subtype.lower() in {'wav'}:
            self.subtype = 'x-wav'

        if param.startswith('charset='):
            self.encoding = param.replace('charset=', '').strip()
        else:
            self.encoding = 'utf8'

    def __eq__(self, other):
        return self.string == other if isinstance(other, str) else \
            (self.type, self.subtype) == (other.type, other.subtype)

    @property
    def is_text(self) -> bool:
        """Whether the mimetype describes text, and hence data should be read as str."""
        return self.type == 'text'

    @property
    def extension(self) -> Union[None, str]:
        """Return a suitable filename extension for the mimetype."""
        return mimetypes.guess_extension(f'{self.type}/{self.subtype}')

    def read(self, data: bytes) -> StrOrBytes:
        """Read data, inferring the encoding from the mimetype."""
        if self.is_text and not isinstance(data, str):
            return data.decode(self.encoding)
        return data

    def write(self, data: StrOrBytes, p: Optional[pathlib.Path] = None) -> Union[int, StrOrBytes]:
        """The mimetype dictates how/if to encode data."""
        res = data.encode(self.encoding) if self.is_text else data
        return p.write_bytes(res) if p else res


def read_data_url(url: urllib.parse.ParseResult, mimetype: Mimetype) -> StrOrBytes:
    """Read data from a data:// URL."""
    spec, _, data = url.path.partition(',')
    if spec.endswith(';base64'):
        data = base64.b64decode(data)

    data = mimetype.read(data)
    if mimetype.is_text:
        data = urllib.parse.unquote(data)
    return data


def read_file_url(d: PathType, url: urllib.parse.ParseResult, mimetype: Mimetype) -> StrOrBytes:
    """Read data from a file:// URL."""
    path = url.path
    while path.startswith('/'):
        path = path[1:]
    if isinstance(d, str):  # pragma: no cover
        # We are accessing media of dataset which has been accessed over HTTP.
        assert urllib.parse.urlparse(d).scheme.startswith('http')
        return read_http_url(urllib.parse.urlparse(urllib.parse.urljoin(d, path)), mimetype)

    return mimetype.read(d.joinpath(urllib.parse.unquote(path)).read_bytes())


def read_http_url(url: urllib.parse.ParseResult, mimetype: Mimetype) -> StrOrBytes:
    """Read data from an HTTP URL."""
    return mimetype.read(urllib.request.urlopen(urllib.parse.urlunparse(url)).read())
