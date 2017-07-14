# coding: utf8
from __future__ import unicode_literals, print_function, division
import sys

from six import string_types
import attr
from clldutils.path import Path
from clldutils.csvw.metadata import TableGroup, Table
from clldutils.misc import log_or_raise
from clldutils import jsonlib

from pycldf.sources import Sources
from pycldf.util import pkg_path
from pycldf.terms import term_uri, TERMS
from pycldf.validators import VALIDATORS

MD_SUFFIX = '-metadata.json'


#
# TODO: support components! -> languages.csv, examples.csv
# validate examples?! have validate call each components/columns validate method?
# - register validators
#   - per propertyUrl on columns
#      latitude, longitude, glottocode, iso639P3code
#   - per dc:conformsTo on tables
# def validator(table, row, column, value):
#


@attr.s
class Module(object):
    uri = attr.ib(validator=attr.validators.in_([t.uri for t in TERMS.classes.values()]))
    fname = attr.ib()
    cls = attr.ib(default=None)

    @property
    def id(self):
        return self.uri.split('#')[1]

    def match(self, thing):
        if isinstance(thing, TableGroup):
            return thing.common_props.get('dc:conformsTo') == term_uri(self.id)
        if hasattr(thing, 'name'):
            return thing.name == self.fname
        return False


_modules = []


def get_modules():
    if not _modules:
        ds = sys.modules[__name__]
        for p in pkg_path('modules').glob('*{0}'.format(MD_SUFFIX)):
            tg = TableGroup.from_file(p)
            mod = Module(tg.common_props['dc:conformsTo'], tg.tables[0].url.string)
            mod.cls = getattr(ds, mod.id)
            _modules.append(mod)
    return _modules


class Dataset(object):
    """
    API to access a CLDF dataset.
    """
    def __init__(self, tablegroup):
        self._tg = tablegroup
        self.sources = Sources.from_file(self.bibpath)

    def add_component(self, component):
        if isinstance(component, string_types):
            component = jsonlib.load(
                pkg_path('components', '{0}{1}'.format(component, MD_SUFFIX)))
        if isinstance(component, dict):
            component = Table.fromvalue(component)
        assert isinstance(component, Table)
        self._tg.tables.append(component)
        component._parent = self._tg
        # FIXME: need a way to declare foreign keys, e.g. for examples!?

    @property
    def bibpath(self):
        return self.directory.joinpath(
            self._tg.common_props.get('dc:source', 'sources.bib'))

    def validate(self, log=None):
        default_tg = TableGroup.from_file(
            pkg_path('modules', '{0}{1}'.format(self.module, MD_SUFFIX)))
        for default_table in default_tg.tables:
            table = self[default_table.common_props['dc:conformsTo']]
            if not table:
                log_or_raise('{0} requires {1}'.format(
                    self.module, default_table.common_props['dc:conformsTo']), log=log)
            else:
                default_cols = set(c.name for c in default_table.tableSchema.columns
                                   if c.required or c.common_props.get('dc:isRequiredBy'))
                for col in default_cols - set(c.name for c in table.tableSchema.columns):
                    log_or_raise('{0} requires column {1}'.format(
                        table.common_props['dc:conformsTo'], col), log=log)

        # check whether self._tg.common_props['dc:conformsTo'] is in validators!
        self._tg.check_referential_integrity(log=log)
        for table in self.tables:
            # check whether table.common_props['dc:conformsTo'] is in validators!
            validators = []
            for col in table.tableSchema.columns:
                if col.propertyUrl:
                    if col.propertyUrl.uri in VALIDATORS:
                        validators.append((col, VALIDATORS[col.propertyUrl.uri]))

            table.check_primary_key(log=log)
            for fname, lineno, row in table.iterdicts(log=log, with_metadata=True):
                for col, validate in validators:
                    try:
                        validate(self, table, col, row)
                    except ValueError as e:
                        log_or_raise(
                            '{0}:{1}:{2} {3}'.format(fname.name, lineno, col.name, e),
                            log=log)

    @property
    def directory(self):
        return self._tg._fname.parent

    @property
    def module(self):
        return self._tg.common_props['dc:conformsTo'].split('#')[1]

    @property
    def version(self):
        return self._tg.common_props['dc:conformsTo'].split('/')[3]

    @classmethod
    def in_dir(cls, d):
        fname = Path(d)
        if not fname.exists():
            fname.mkdir()
        assert fname.is_dir()
        return cls.from_metadata(fname)

    @classmethod
    def from_metadata(cls, fname):
        fname = Path(fname)
        if fname.is_dir():
            name = '{0}{1}'.format(cls.__name__, MD_SUFFIX)
            tablegroup = TableGroup.from_file(pkg_path('modules', name))
            # adapt the path of the metadata file such that paths to tables are resolved
            # correctly:
            tablegroup._fname = fname.joinpath(name)
        else:
            tablegroup = TableGroup.from_file(fname)

        for mod in get_modules():
            if mod.match(tablegroup):
                return mod.cls(tablegroup)
        return cls(tablegroup)

    @classmethod
    def from_data(cls, fname):
        fname = Path(fname)
        for mod in get_modules():
            if mod.match(fname):
                return mod.cls.from_metadata(fname if fname.is_dir() else fname.parent)
        raise ValueError(fname)

    def __repr__(self):
        return '<cldf:%s:%s at %s>' % (self.version, self.module, self.directory)

    def __getitem__(self, type_):
        """
        Tables can be accessed by type.
        """
        type_ = term_uri(type_)
        for table in self._tg.tables:
            if table.common_props.get('dc:conformsTo') == type_:
                return table

    @property
    def tables(self):
        return self._tg.tables

    def stats(self):
        res = []
        for table in self.tables:
            dctype = table.common_props.get('dc:conformsTo')
            if dctype.split('#')[1] in TERMS:
                dctype = TERMS[dctype.split('#')[1]].label
            res.append((table.url.string, dctype, len(list(table))))
        if self.sources:
            res.append((self.bibpath, 'Sources', len(self.sources)))
        return res

    def write_metadata(self, fname=None):
        return self._tg.to_file(fname or self._tg._fname)

    def write_sources(self):
        return self.sources.write(self.bibpath)

    def write(self, fname=None, **table_items):
        if self.sources and not self._tg.common_props.get('dc:source'):
            self._tg.common_props['dc:source'] = 'sources.bib'
        self.write_metadata(fname)
        self.write_sources()
        for table_type, items in table_items.items():
            self[table_type].write(items)


class Wordlist(Dataset):
    pass


class Dictionary(Dataset):
    pass


class StructureDataset(Dataset):
    pass
