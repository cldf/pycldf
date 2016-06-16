# coding: utf8
from __future__ import unicode_literals, print_function, division
from itertools import chain
import re
from collections import Counter

from six import text_type, string_types
from pybtex import database
from pybtex.database.output.bibtex import Writer as BaseWriter

from pycldf.util import OptionalFile


class Writer(BaseWriter):
    def quote(self, s):
        self.check_braces(s)
        return '{%s}' % s


class GlottologRef(text_type):
    @staticmethod
    def is_valid(s):
        return re.match('[1-9][0-9]*$', s)


def itersources(s):
    for spec in s.split(';'):
        spec = spec.strip()
        if spec:
            sid, pages = spec, None
            if '[' in spec:
                sid, pages = [ss.strip() for ss in spec.split('[', 1)]
                assert sid and pages.endswith(']')
                pages = pages[:-1].strip()
            yield GlottologRef(sid) if GlottologRef.is_valid(sid) else sid, pages


class Source(database.Entry):
    def __init__(self, key, bibtex_type='misc', **kw):
        persons = dict(
            author=list(self.persons(kw.pop('author', ''))),
            editor=list(self.persons(kw.pop('editor', ''))))
        database.Entry.__init__(self, bibtex_type, fields=kw, persons=persons)
        self.key = key

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


class Sources(OptionalFile):
    def __init__(self):
        self.bibdata = database.BibliographyData()
        self.glottolog_refs = set()

    def keys(self):
        return list(chain(self.glottolog_refs, self.bibdata.entries))

    def check(self, *rowdicts):
        keys = self.keys()
        c = Counter()
        for d in rowdicts:
            for rid, _ in itersources(d.get('Source', '')):
                if isinstance(rid, GlottologRef):
                    self.glottolog_refs.add(rid)
                else:
                    assert rid in keys
                c.update([rid])
        return c

    def _add_entries(self, data):
        if isinstance(data, Source):
            entries = [(data.key, data)]
        elif isinstance(data, database.BibliographyData):
            entries = data.entries.items()
        else:
            raise ValueError(data)

        for key, entry in entries:
            if key not in self.bibdata.entries:
                try:
                    self.bibdata.add_entry(key, entry)
                except database.BibliographyDataError as e:  # pragma: no cover
                    raise ValueError('%s' % e)

    def read(self, fname):
        self._add_entries(database.parse_file(fname.as_posix(), bib_format='bibtex'))

    def write(self, fname):
        Writer().write_file(self.bibdata, fname.as_posix())

    def add(self, *entries):
        """
        Add a source, either specified by glottolog reference id, or as bibtex record.
        """
        for entry in entries:
            if isinstance(entry, string_types):
                self._add_entries(database.parse_string(entry, bib_format='bibtex'))
            else:
                self._add_entries(entry)
