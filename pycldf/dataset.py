# coding: utf8
from __future__ import unicode_literals, print_function, division
import sys
from itertools import chain

from six import string_types
import attr
from clldutils.path import Path
from clldutils.csvw.metadata import TableGroup, Table, Column, ForeignKey, URITemplate
from clldutils.misc import log_or_raise
from clldutils import jsonlib

from pycldf.sources import Sources
from pycldf.util import pkg_path, multislice
from pycldf.terms import term_uri, TERMS
from pycldf.validators import VALIDATORS

MD_SUFFIX = '-metadata.json'


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

    @property
    def tablegroup(self):
        return self._tg

    @property
    def metadata_dict(self):
        return self.tablegroup.asdict(omit_defaults=False)

    @property
    def properties(self):
        return self.tablegroup.common_props

    @property
    def tables(self):
        return self.tablegroup.tables

    def add_component(self, component, *cols):
        if isinstance(component, string_types):
            component = jsonlib.load(
                pkg_path('components', '{0}{1}'.format(component, MD_SUFFIX)))
        if isinstance(component, dict):
            component = Table.fromvalue(component)
        assert isinstance(component, Table)
        for col in cols:
            if isinstance(col, string_types):
                col_ = Column(name=col, datatype='string')
                if col in TERMS:
                    col_.propertyUrl = URITemplate(TERMS[col].uri)
                col = col_
            elif isinstance(col, dict):
                col = Column.fromvalue(col)
            assert isinstance(col, Column)
            component.tableSchema.columns.append(col)
        table_type = self.get_tabletype(component)
        for table in self.tables:
            if self.get_tabletype(table) and self.get_tabletype(table) == table_type:
                raise ValueError('components must not be added twice')

        self.tables.append(component)
        component._parent = self._tg

        if table_type:
            fkey_name = '{0}_ID'.format(table_type.replace('Table', ''))
            for table in self.tables:
                schema = table.tableSchema
                for col in schema.columns:
                    if col.name == fkey_name:
                        for fkey in schema.foreignKeys:
                            if fkey.columnReference == [fkey_name]:
                                break
                        else:
                            schema.foreignKeys.append(ForeignKey.fromdict(dict(
                                columnReference=fkey_name,
                                reference=dict(
                                    resource=component.url.string,
                                    columnReference='ID'))))
                        break

    @property
    def bibpath(self):
        return self.directory.joinpath(self.properties.get('dc:source', 'sources.bib'))

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

        data = {}
        for table in self.tables:
            type_uri = table.common_props.get('dc:conformsTo')
            if type_uri:
                try:
                    TERMS.is_cldf_uri(table.common_props.get('dc:conformsTo'))
                except ValueError:
                    log_or_raise('invalid CLDF URI: {0}'.format(type_uri), log=log)

            # FIXME: check whether table.common_props['dc:conformsTo'] is in validators!
            validators = []
            for col in table.tableSchema.columns:
                if col.propertyUrl:
                    try:
                        TERMS.is_cldf_uri(col.propertyUrl.uri)
                    except ValueError:
                        log_or_raise(
                            'invalid CLDF URI: {0}'.format(col.propertyUrl.uri), log=log)
                    if col.propertyUrl.uri in VALIDATORS:
                        validators.append((col, VALIDATORS[col.propertyUrl.uri]))

            data[table.local_name] = []
            for fname, lineno, row in table.iterdicts(log=log, with_metadata=True):
                data[table.local_name].append((fname, lineno, row))
                for col, validate in validators:
                    try:
                        validate(self, table, col, row)
                    except ValueError as e:
                        log_or_raise(
                            '{0}:{1}:{2} {3}'.format(fname.name, lineno, col.name, e),
                            log=log)
            table.check_primary_key(log=log, items=data[table.local_name])

        self._tg.check_referential_integrity(log=log, data=data)

    @property
    def directory(self):
        return self._tg._fname.parent

    @property
    def module(self):
        return self.properties['dc:conformsTo'].split('#')[1]

    @property
    def version(self):
        return self.properties['dc:conformsTo'].split('/')[3]

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
        for table in self.tables:
            if table.common_props.get('dc:conformsTo') == type_:
                return table
        raise KeyError(table)

    @staticmethod
    def get_tabletype(table):
        if '#' in table.common_props.get('dc:conformsTo', ''):
            res = table.common_props['dc:conformsTo'].split('#')[1]
            if res in TERMS:
                return res
        raise KeyError(table)

    @property
    def primary_table(self):
        if self.tables:
            return self.get_tabletype(self.tables[0])

    def stats(self):
        res = []
        for table in self.tables:
            dctype = table.common_props.get('dc:conformsTo')
            if dctype.split('#')[1] in TERMS:
                dctype = TERMS[dctype.split('#')[1]].label
            res.append((table.url.string, dctype, len(list(table))))
        if self.sources:
            res.append((self.bibpath.name, 'Sources', len(self.sources)))
        return res

    def write_metadata(self, fname=None):
        return self._tg.to_file(fname or self._tg._fname)

    def write_sources(self):
        return self.sources.write(self.bibpath)

    def write(self, fname=None, **table_items):
        if self.sources and not self.properties.get('dc:source'):
            self.properties['dc:source'] = 'sources.bib'
        self.write_metadata(fname)
        self.write_sources()
        for table_type, items in table_items.items():
            self[table_type].write(items)


class Wordlist(Dataset):
    @property
    def primary_table(self):
        return 'FormTable'

    def get_soundsequence(self, row, table='FormTable'):
        col = self[table].get_column(
            "http://cldf.clld.org/v1.0/terms.rdf#soundSequence")
        sounds = row[col.name]
        if isinstance(sounds, string_types):
            # This may be the case when no morpheme boundaries are provided.
            sounds = [sounds]
        return list(chain(*[s.split() for s in sounds]))

    def get_subsequence(self, partial_cognate):
        """
        Compute the subsequence of the morphemes of a form which is specified in a partial
        cognate assignment.

        :param partial_cognate:
        :return:
        """
        # 1. Determine the "slices" column in the PartialCognateTable
        slices = self['PartialCognateTable'].get_column(
            "http://cldf.clld.org/v1.0/terms.rdf#slice")

        # 2. Determine the "soundSequence" column in the FormTable
        morphemes = self['FormTable'].get_column(
            "http://cldf.clld.org/v1.0/terms.rdf#soundSequence")

        # 3. Retrieve the matching row in FormTable
        for row in self['FormTable']:
            if row['ID'] == partial_cognate['Form_ID']:
                break
        else:
            raise ValueError(partial_cognate['Form_ID'])  # pragma: no cover

        # 4. Slice the segments
        return list(chain(*[
            s.split() for s in multislice(
                row[morphemes.name], *partial_cognate[slices.name])]))


class Dictionary(Dataset):
    @property
    def primary_table(self):
        return 'EntryTable'


class StructureDataset(Dataset):
    @property
    def primary_table(self):
        return 'ValueTable'
