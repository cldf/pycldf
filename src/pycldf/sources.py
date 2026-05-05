"""
Functionality to handle BibTeX source data of Datasets.
"""
import re
import types
from typing import Optional, Union, Literal
import pathlib
import zipfile
import tempfile
import collections
from urllib.error import HTTPError
from urllib.request import urlopen, urlretrieve
from collections.abc import Generator, Iterable, KeysView

from csvw.metadata import is_url
from simplepybtex import database
from simplepybtex.database.output.bibtex import Writer as BaseWriter
from clldutils.source import Source as BaseSource
from clldutils.source import ID_PATTERN

from pycldf.urlutil import update_url
from pycldf.fileutil import PathType

__all__ = ['Source', 'Sources', 'Reference']

GLOTTOLOG_ID_PATTERN = re.compile('^[1-9][0-9]*$')


class Writer(BaseWriter):
    """We overwrite pybtex's writer to ensure data is wrapped in curly braces."""
    def quote(self, s):
        self.check_braces(s)
        return '{%s}' % s

    def _encode(self, text):
        #
        # FIXME: We overwrite a private method here!  pylint: disable=fixme
        #
        return text


class Source(BaseSource):
    """
    A bibliograhical record, specifying a source for some data in a CLDF dataset.
    """
    @property
    def entry(self) -> database.Entry:
        """Converts Source to a pybtex Entry."""
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
        return f'<{self.__class__.__name__} {self.id}>'

    @classmethod
    def from_entry(cls, key: str, entry: database.Entry, **_kw):
        """
        Create a `cls` instance from a `simplepybtex` entry object.

        :param key: BibTeX citation key of the entry
        :param entry: `simplepybtex.database.Entry` instance
        :param _kw: Non-bib-metadata keywords to be passed for `cls` instantiation
        :return: `cls` instance
        """
        _kw.update(entry.fields.items())
        _kw.setdefault('_check_id', False)
        for role in entry.persons:
            if entry.persons[role]:
                _kw[role] = ' and '.join(f'{p}' for p in entry.persons[role])
        return cls(entry.type, key, **_kw)

    @staticmethod
    def persons(s: str) -> Generator[database.Person, None, None]:
        """Yields persons encoded in an author names string."""
        for name in re.split(r'\s+&\s+|\s+and\s+', s.strip()):
            if name:
                parts = name.split(',')
                if len(parts) > 2:
                    for part in parts:
                        yield database.Person(part.strip())
                else:
                    yield database.Person(name)

    def refkey(self, year_brackets: Union[None, Literal["round", "square", "curly"]] = 'round'):
        """Compute an author-year type reference key for the item."""
        brackets = {
            None: ('', ''),
            'round': ('(', ')'),
            'square': ('[', ']'),
            'curly': ('{', '}')}.get(year_brackets)
        persons = self.entry.persons.get('author') or self.entry.persons.get('editor', [])
        names = ' '.join(persons[0].prelast_names + persons[0].last_names) if persons else 'n.a.'
        if len(persons) == 2:
            names += f" and {' '.join(persons[1].last_names)}"
        elif len(persons) > 2:
            names += ' et al.'
        names = names.replace('{', '').replace('}', '')
        return f"{names} {brackets[0]}{self.get('year', 'n.d.')}{brackets[1]}"


class Reference:
    """
    A reference connects a piece of data with a `Source`, typically adding some citation context \
    often page numbers, or similar.
    """
    def __init__(self, source: Source, desc: Optional[str]):
        if desc and ('[' in desc or ']' in desc or ';' in desc):
            raise ValueError(f'invalid ref description: {desc}')
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
            res += f'[{self.description}]'
        return res

    def __repr__(self):
        return f'<{self.__class__.__name__} {self}>'


class Sources:
    """
    A `dict` like container for all sources linked to data in a CLDF dataset.
    """
    def __init__(self):
        self._bibdata = database.BibliographyData()

    @classmethod
    def from_file(cls, fname: PathType) -> 'Sources':
        """Instantiate an instance from the data in a BibTeX file."""
        zipped = False
        res = cls()
        if not is_url(str(fname)):
            fname = pathlib.Path(fname)
            if not fname.exists():
                fname = fname.parent / f'{fname.name}.zip'
                zipped = True
            if fname.exists():
                assert fname.is_file(), f'Bibfile {fname} must be a file!'
                res.read(fname, zipped=zipped)
        else:
            res.read(fname)
        return res

    def __bool__(self):
        return bool(self._bibdata.entries)

    __nonzero__ = __bool__

    def keys(self) -> KeysView[str]:  # pylint: disable=C0116
        return self._bibdata.entries.keys()

    def items(self) -> Generator[Source, None, None]:  # pylint: disable=C0116
        for key, entry in self._bibdata.entries.items():
            yield Source.from_entry(key, entry)

    def __iter__(self):
        return self.items()

    def __len__(self) -> int:
        return len(self._bibdata.entries)

    def __getitem__(self, item: str) -> Optional[Source]:
        try:
            return Source.from_entry(item, self._bibdata.entries[item])
        except KeyError as e:
            raise ValueError(f'missing citekey: {item}') from e

    def __contains__(self, item: str) -> bool:
        return item in self._bibdata.entries

    @staticmethod
    def format_refs(*refs) -> list[str]:  # pylint: disable=C0116
        return [f'{ref}' for ref in refs]

    @staticmethod
    def parse(ref: str) -> tuple[str, str]:
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

    def validate(self, refs: Union[str, list[str]]) -> None:
        """Make sure refs are valid. If not, raises Exceptions."""
        if not isinstance(refs, str) and any(r is None for r in refs):
            raise ValueError('empty reference in ref list (possibly caused by trailing separator)')
        for sid, _ in map(self.parse, [refs] if isinstance(refs, str) else refs):
            if sid not in self.keys():
                raise ValueError(f'missing source key: {sid}')

    def expand_refs(self, refs: Iterable[str], **kw) -> Iterable[Reference]:
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

    def _add_entries(self, data: Union[Source, database.BibliographyData], **kw) -> None:
        if isinstance(data, Source):
            entries = [(data.id, data.entry)]
        elif hasattr(data, 'entries'):
            entries = data.entries.items()
        else:
            msg = (
                'expected `clldutils.source.Source`,'
                ' `pybtex.database.BibliographyData`,'
                ' or `simplepybtex.database.BibliographyData`;'
                f' got {type(data)}')
            raise TypeError(msg)

        for key, entry in entries:
            if kw.get('_check_id', False) and not ID_PATTERN.match(key):
                raise ValueError(f'invalid source ID: {key}')
            if key not in self._bibdata.entries:
                try:
                    self._bibdata.add_entry(key, entry)
                except database.BibliographyDataError as e:  # pragma: no cover
                    raise ValueError(f'{e}') from e

    def read(self, fname: PathType, zipped=False, **kw):
        """Read sources from a BibTex file (possibly specified via URL)."""
        if is_url(str(fname)):
            fname = str(fname)
            try:
                with urlopen(fname) as f:
                    content = f.read().decode('utf-8')
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
                with zipfile.ZipFile(str(fname), 'r') as zf:
                    content = zf.read(zf.namelist()[0]).decode('utf8')
            else:
                content = pathlib.Path(fname).read_text(encoding='utf-8')
        self._add_entries(
            database.parse_string(content, bib_format='bibtex'), **kw)

    def write(self, fname: PathType, ids=None, zipped=False, **_) -> Optional[pathlib.Path]:
        """Write sources to a file (if there are any)."""
        if ids:
            bibdata = database.BibliographyData()
            for key, entry in self._bibdata.entries.items():
                if key in ids:
                    bibdata.add_entry(key, entry)
        else:
            bibdata = self._bibdata
        fname = pathlib.Path(fname)
        if bibdata.entries:
            with fname.open('w', encoding='utf8') as fp:
                Writer().write_stream(bibdata, fp)
            if zipped:
                with zipfile.ZipFile(
                        fname.parent / f'{fname.name}.zip',
                        'w',
                        compression=zipfile.ZIP_DEFLATED) as zf:
                    zf.write(fname, fname.name)
                fname.unlink()
            return fname
        return None

    def add(self, *entries: Union[str, Source], **kw) -> None:
        """
        Add a source, either specified as BibTeX string or as :class:`Source`.
        """
        for entry in entries:
            if isinstance(entry, str):
                self._add_entries(database.parse_string(entry, bib_format='bibtex'), **kw)
            else:
                self._add_entries(entry, **kw)
