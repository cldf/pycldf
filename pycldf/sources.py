# coding: utf8
from __future__ import unicode_literals, print_function, division
import re

from six import string_types, StringIO
from pybtex import database
from pybtex.database.output.bibtex import Writer as BaseWriter
from clldutils.misc import UnicodeMixin
from clldutils.source import Source as BaseSource
from clldutils.source import ID_PATTERN

from pycldf.util import OptionalData

__all__ = ['Source']

GLOTTOLOG_ID_PATTERN = re.compile('^[1-9][0-9]*$')


class Writer(BaseWriter):
    def quote(self, s):
        self.check_braces(s)
        return '{%s}' % s


class Source(BaseSource, UnicodeMixin):
    def __init__(self, genre, id_, **kw):
        BaseSource.__init__(self, genre, id_, **kw)
        persons = dict(
            author=list(self.persons(kw.pop('author', ''))),
            editor=list(self.persons(kw.pop('editor', ''))))
        assert 'author' not in kw
        self.entry = database.Entry(
            genre,
            fields={k: v for k, v in kw.items() if v},
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


class Sources(OptionalData):
    def __init__(self):
        self._bibdata = database.BibliographyData()

    def keys(self):
        return self._bibdata.entries.keys()

    def items(self):
        for key, entry in self._bibdata.entries.items():
            yield Source.from_entry(key, entry)

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
        return ';'.join('%s' % ref for ref in refs)

    def expand_refs(self, refs):
        refs = refs or ''
        for spec in refs.split(';'):
            spec = spec.strip()
            if spec:
                sid, pages = spec, None
                if '[' in spec:
                    sid, pages = [ss.strip() for ss in spec.split('[', 1)]
                    assert sid and pages.endswith(']')
                    pages = pages[:-1].strip()
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

    def read_string(self, text):
        self._add_entries(database.parse_string(text, bib_format='bibtex'))

    def write_string(self, ids=None, **kw):
        if ids:
            bibdata = database.BibliographyData()
            for key, entry in self._bibdata.entries.items():
                if key in ids:
                    bibdata.add_entry(key, entry)
        else:
            bibdata = self._bibdata
        if bibdata.entries:
            out = StringIO()
            Writer().write_stream(bibdata, out)
            out.seek(0)
            return out.read()

    def add(self, *entries):
        """
        Add a source, either specified by glottolog reference id, or as bibtex record.
        """
        for entry in entries:
            if isinstance(entry, string_types):
                self._add_entries(database.parse_string(entry, bib_format='bibtex'))
            else:
                self._add_entries(entry)
