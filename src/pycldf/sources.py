import re
import types
import typing
import pathlib
import zipfile
import tempfile
import collections
from urllib.error import HTTPError
from urllib.request import urlopen, urlretrieve

from csvw.metadata import is_url
from pybtex import database
from pybtex.database.output.bibtex import Writer as BaseWriter
from clldutils.source import Source as BaseSource
from clldutils.source import ID_PATTERN

from pycldf.util import update_url

__all__ = ['Source', 'Sources', 'Reference']

GLOTTOLOG_ID_PATTERN = re.compile('^[1-9][0-9]*$')


class Writer(BaseWriter):
    def quote(self, s):
        self.check_braces(s)
        return '{%s}' % s

    def _encode(self, text):
        #
        # FIXME: We overwrite a private method here!
        #
        return text


class Source(BaseSource):
    """
    A bibliograhical record, specifying a source for some data in a CLDF dataset.
    """
    @property
    def entry(self):
        persons = collections.OrderedDict([
            ('author', list(self.persons(self.get('author', '')))),
            ('editor', list(self.persons(self.get('editor', '')))),
        ])
        return database.Entry(
            self.genre,
            fields=collections.OrderedDict(
                (k, v) for k, v in sorted(self.items()) if v and k not in ['author', 'editor']),
            persons=persons)

    def __str__(self):
        return self.text()

    def __repr__(self):
        return '<%s %s>' % (self.__class__.__name__, self.id)

    @classmethod
    def from_entry(cls, key, entry, **_kw):
        """
        Create a `cls` instance from a `pybtex` entry object.

        :param key: BibTeX citation key of the entry
        :param entry: `pybtex.database.Entry` instance
        :param _kw: Non-bib-metadata keywords to be passed for `cls` instantiation
        :return: `cls` instance
        """
        _kw.update({k: v for k, v in entry.fields.items()})
        _kw.setdefault('_check_id', False)
        for role in entry.persons:
            if entry.persons[role]:
                _kw[role] = ' and '.join('%s' % p for p in entry.persons[role])
        return cls(entry.type, key, **_kw)

    @staticmethod
    def persons(s):
        for name in re.split(r'\s+&\s+|\s+and\s+', s.strip()):
            if name:
                parts = name.split(',')
                if len(parts) > 2:
                    for part in parts:
                        yield database.Person(part.strip())
                else:
                    yield database.Person(name)

    def refkey(self, year_brackets='round'):
        brackets = {None: ('', ''), 'round': ('(', ')'), 'square': ('[', ']'), 'curly': ('{', '}')}
        persons = self.entry.persons.get('author') or self.entry.persons.get('editor', [])
        s = ' '.join(persons[0].prelast_names + persons[0].last_names) if persons else 'n.a.'
        if len(persons) == 2:
            s += ' and {}'.format(' '.join(persons[1].last_names))
        elif len(persons) > 2:
            s += ' et al.'
        return s.replace('{', '').replace('}', '') + ' {}{}{}'.format(
            brackets[year_brackets][0], self.get('year', 'n.d.'), brackets[year_brackets][1])


class Reference(object):
    """
    A reference connects a piece of data with a `Source`, typically adding some citation context \
    often page numbers, or similar.
    """
    def __init__(self, source: Source, desc: typing.Union[str, None]):
        if desc and ('[' in desc or ']' in desc or ';' in desc):
            raise ValueError('invalid ref description: %s' % desc)
        self.source = source
        self.fields = types.SimpleNamespace(**self.source) if isinstance(self.source, dict) else {}
        self.description = desc

    def __str__(self):
        """
        String representation of a reference according to the CLDF specification.

        .. seealso:: https://github.com/cldf/cldf#sources
        """
        res = self.source.id if hasattr(self.source, 'id') else self.source
        if self.description:
            res += '[%s]' % self.description
        return res

    def __repr__(self):
        return '<%s %s>' % (self.__class__.__name__, self)


class Sources(object):
    """
    A `dict` like container for all sources linked to data in a CLDF dataset.
    """
    def __init__(self):
        self._bibdata = database.BibliographyData()

    @classmethod
    def from_file(cls, fname):
        zipped = False
        res = cls()
        if not is_url(fname):
            fname = pathlib.Path(fname)
            if not fname.exists():
                fname = fname.parent / '{}.zip'.format(fname.name)
                zipped = True
            if fname.exists():
                assert fname.is_file(), 'Bibfile {} must be a file!'.format(fname)
                res.read(fname, zipped=zipped)
        else:
            res.read(fname)
        return res

    def __bool__(self):
        return bool(self._bibdata.entries)

    __nonzero__ = __bool__

    def keys(self):
        return self._bibdata.entries.keys()

    def items(self):
        for key, entry in self._bibdata.entries.items():
            yield Source.from_entry(key, entry)

    def __iter__(self):
        return self.items()

    def __len__(self):
        return len(self._bibdata.entries)

    def __getitem__(self, item):
        try:
            return Source.from_entry(item, self._bibdata.entries[item])
        except KeyError:
            raise ValueError('missing citekey: %s' % item)

    def __contains__(self, item):
        return item in self._bibdata.entries

    @staticmethod
    def format_refs(*refs):
        return ['%s' % ref for ref in refs]

    @staticmethod
    def parse(ref: str) -> typing.Tuple[str, str]:
        """
        Parse the string representation of a reference into source ID and context.

        :raises ValueError: if the reference does not match the expected format.
        """
        sid, pages = ref.strip(), None
        if '[' in sid:
            sid, pages = [ss.strip() for ss in sid.split('[', 1)]
            if not (sid and pages.endswith(']')):
                raise ValueError(ref)
            pages = pages[:-1].strip()
        return sid, pages

    def validate(self, refs):
        for sid, _ in map(self.parse, [refs] if isinstance(refs, str) else refs):
            if sid not in self.keys():
                raise ValueError('missing source key: {0}'.format(sid))

    def expand_refs(self, refs: typing.Iterable[str], **kw) -> typing.Iterable[Reference]:
        """
        Turn a list of string references into proper :class:`Reference` instances, looking up \
        sources in `self`.

        This can be used from a :class:`pycldf.Dataset` as follows:

        .. code-block:: python

            >>> for row in dataset.iter_rows('ValueTable', 'source'):
            ...     for ref in dataset.sources.expand_refs(row['source']):
            ...         print(ref.source)
        """
        for sid, pages in map(
                self.parse, [refs] if isinstance(refs, str) else refs):
            if sid not in self and GLOTTOLOG_ID_PATTERN.match(sid):
                self._add_entries(Source('misc', sid, glottolog_id=sid), **kw)
            yield Reference(self[sid], pages)

    def _add_entries(self, data, **kw):
        if isinstance(data, Source):
            entries = [(data.id, data.entry)]
        elif isinstance(data, database.BibliographyData):
            entries = data.entries.items()
        else:
            raise ValueError(data)

        for key, entry in entries:
            if kw.get('_check_id', False) and not ID_PATTERN.match(key):
                raise ValueError('invalid source ID: %s' % key)
            if key not in self._bibdata.entries:
                try:
                    self._bibdata.add_entry(key, entry)
                except database.BibliographyDataError as e:  # pragma: no cover
                    raise ValueError('%s' % e)

    def read(self, fname, zipped=False, **kw):
        if is_url(fname):
            try:
                content = urlopen(fname).read().decode('utf-8')
            except HTTPError as e:
                if '404' in str(e):
                    fname = update_url(
                        fname, lambda u: (u.scheme, u.netloc, u.path + '.zip', u.query, u.fragment))
                    with tempfile.TemporaryDirectory() as tmp:
                        zfname = pathlib.Path(tmp) / 'sources.zip'
                        urlretrieve(fname, zfname)
                        with zipfile.ZipFile(zfname, 'r') as zf:
                            content = zf.read(zf.namelist()[0]).decode('utf8')
        else:
            if zipped:
                with zipfile.ZipFile(fname, 'r') as zf:
                    content = zf.read(zf.namelist()[0]).decode('utf8')
            else:
                content = pathlib.Path(fname).read_text(encoding='utf-8')
        self._add_entries(
            database.parse_string(content, bib_format='bibtex'), **kw)

    def write(self, fname, ids=None, zipped=False, **kw):
        if ids:
            bibdata = database.BibliographyData()
            for key, entry in self._bibdata.entries.items():
                if key in ids:
                    bibdata.add_entry(key, entry)
        else:
            bibdata = self._bibdata
        if bibdata.entries:
            with pathlib.Path(fname).open('w', encoding='utf8') as fp:
                Writer().write_stream(bibdata, fp)
            if zipped:
                with zipfile.ZipFile(
                        fname.parent / '{}.zip'.format(fname.name),
                        'w',
                        compression=zipfile.ZIP_DEFLATED) as zf:
                    zf.write(fname, fname.name)
                fname.unlink()
            return fname

    def add(self, *entries: typing.Union[str, Source], **kw):
        """
        Add a source, either specified as BibTeX string or as :class:`Source`.
        """
        for entry in entries:
            if isinstance(entry, str):
                self._add_entries(database.parse_string(entry, bib_format='bibtex'), **kw)
            else:
                self._add_entries(entry, **kw)
