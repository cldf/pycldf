# coding: utf8
from __future__ import unicode_literals, print_function, division
import re

from six import string_types
from pybtex import database
from pybtex.database.output.bibtex import Writer as BaseWriter
from clldutils.path import Path, read_text
from clldutils.misc import UnicodeMixin
from clldutils.source import Source as BaseSource
from clldutils.source import ID_PATTERN

__all__ = ['Source', 'Reference']

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


class Source(BaseSource, UnicodeMixin):
    @property
    def entry(self):
        persons = dict(
            author=list(self.persons(self.get('author', ''))),
            editor=list(self.persons(self.get('editor', ''))))
        return database.Entry(
            self.genre,
            fields={k: v for k, v in self.items() if v and k not in ['author', 'editor']},
            persons=persons)

    def __unicode__(self):
        return self.text()

    def __repr__(self):
        return '<%s %s>' % (self.__class__.__name__, self.id)

    @classmethod
    def from_entry(cls, key, entry):
        kw = {k: v for k, v in entry.fields.items()}
        for role in entry.persons:
            if entry.persons[role]:
                kw[role] = ' and '.join('%s' % p for p in entry.persons[role])
        return cls(entry.type, key, **kw)

    @staticmethod
    def persons(s):
        for name in re.split('\s+&\s+|\s+and\s+', s.strip()):
            if name:
                parts = name.split(',')
                if len(parts) > 2:
                    for part in parts:
                        yield database.Person(part.strip())
                else:
                    yield database.Person(name)


class Reference(UnicodeMixin):
    def __init__(self, source, desc):
        if desc and ('[' in desc or ']' in desc or ';' in desc):
            raise ValueError('invalid ref description: %s' % desc)
        self.source = source
        self.description = desc

    def __unicode__(self):
        res = self.source.id
        if self.description:
            res += '[%s]' % self.description
        return res

    def __repr__(self):
        return '<%s %s>' % (self.__class__.__name__, self.__unicode__())


class Sources(object):
    def __init__(self):
        self._bibdata = database.BibliographyData()

    @classmethod
    def from_file(cls, fname):
        res = cls()
        fname = Path(fname)
        if fname.exists():
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
    def parse(ref):
        sid, pages = ref.strip(), None
        if '[' in sid:
            sid, pages = [ss.strip() for ss in sid.split('[', 1)]
            if not (sid and pages.endswith(']')):
                raise ValueError(ref)
            pages = pages[:-1].strip()
        return sid, pages

    def validate(self, refs):
        for sid, _ in map(self.parse, [refs] if isinstance(refs, string_types) else refs):
            if sid not in self:
                raise ValueError('missing source key: {0}'.format(sid))

    def expand_refs(self, refs):
        for sid, pages in map(
                self.parse, [refs] if isinstance(refs, string_types) else refs):
            if sid not in self and GLOTTOLOG_ID_PATTERN.match(sid):
                self._add_entries(Source('misc', sid, glottolog_id=sid))
            yield Reference(self[sid], pages)

    def _add_entries(self, data):
        if isinstance(data, Source):
            entries = [(data.id, data.entry)]
        elif isinstance(data, database.BibliographyData):
            entries = data.entries.items()
        else:
            raise ValueError(data)

        for key, entry in entries:
            if not ID_PATTERN.match(key):
                raise ValueError('invalid source ID: %s' % key)
            if key not in self._bibdata.entries:
                try:
                    self._bibdata.add_entry(key, entry)
                except database.BibliographyDataError as e:  # pragma: no cover
                    raise ValueError('%s' % e)

    def read(self, fname):
        self._add_entries(database.parse_string(read_text(fname), bib_format='bibtex'))

    def write(self, fname, ids=None, **kw):
        if ids:
            bibdata = database.BibliographyData()
            for key, entry in self._bibdata.entries.items():
                if key in ids:
                    bibdata.add_entry(key, entry)
        else:
            bibdata = self._bibdata
        if bibdata.entries:
            with Path(fname).open('w', encoding='utf8') as fp:
                Writer().write_stream(bibdata, fp)
            return fname

    def add(self, *entries):
        """
        Add a source, either specified by glottolog reference id, or as bibtex record.
        """
        for entry in entries:
            if isinstance(entry, string_types):
                self._add_entries(database.parse_string(entry, bib_format='bibtex'))
            else:
                self._add_entries(entry)
