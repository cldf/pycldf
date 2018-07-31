# coding: utf8
from __future__ import unicode_literals, print_function, division

import sys
from itertools import chain
from collections import Counter

from six import string_types

import attr
from csvw.metadata import TableGroup, Table, Column, ForeignKey
from csvw.dsv import iterrows
from clldutils.path import Path
from clldutils.misc import log_or_raise
from clldutils import jsonlib

from pycldf.sources import Sources
from pycldf.util import pkg_path, resolve_slices
from pycldf.terms import term_uri, TERMS
from pycldf.validators import VALIDATORS

__all__ = ['Dataset', 'Generic', 'Wordlist', 'ParallelText', 'Dictionary', 'StructureDataset']

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
    global _modules
    if not _modules:
        ds = sys.modules[__name__]
        for p in pkg_path('modules').glob('*{0}'.format(MD_SUFFIX)):
            tg = TableGroup.from_file(p)
            mod = Module(
                tg.common_props['dc:conformsTo'],
                tg.tables[0].url.string if tg.tables else None)
            mod.cls = getattr(ds, mod.id)
            _modules.append(mod)
        # prefer Wordlist over ParallelText (forms.csv)
        _modules = sorted(
            _modules,
            key=lambda m: (m.cls in (Wordlist, ParallelText), m.cls is ParallelText))
    return _modules


def make_column(spec):
    if isinstance(spec, string_types):
        if spec in TERMS.by_uri:
            return TERMS.by_uri[spec].to_column()
        return Column(name=spec, datatype='string')
    if isinstance(spec, dict):
        return Column.fromvalue(spec)
    if isinstance(spec, Column):
        return spec
    raise TypeError(spec)


class Dataset(object):
    """
    API to access a CLDF dataset.
    """

    def __init__(self, tablegroup):
        self.tablegroup = tablegroup
        self.auto_constraints()
        self.sources = Sources.from_file(self.bibpath)

    @property
    def metadata_dict(self):
        return self.tablegroup.asdict(omit_defaults=False)

    @property
    def properties(self):
        return self.tablegroup.common_props

    @property
    def tables(self):
        return self.tablegroup.tables

    def add_sources(self, *sources):
        self.sources.add(*sources)

    def add_table(self, url, *cols):
        self.add_component(
            {"url": url, "tableSchema": {"columns": []}},
            *cols)

    def add_component(self, component, *cols):
        if isinstance(component, string_types):
            component = jsonlib.load(
                pkg_path('components', '{0}{1}'.format(component, MD_SUFFIX)))
        if isinstance(component, dict):
            component = Table.fromvalue(component)
        assert isinstance(component, Table)
        self.add_columns(component, *cols)
        try:
            table_type = self.get_tabletype(component)
        except ValueError:
            table_type = None
        for other_table in self.tables:
            try:
                other_table_type = self.get_tabletype(other_table)
            except ValueError:
                continue
            if other_table_type == table_type:
                raise ValueError('components must not be added twice')

        self.tables.append(component)
        component._parent = self.tablegroup
        self.auto_constraints(component)

    def add_columns(self, table, *cols):
        table = self[table]
        for col in cols:
            existing = [c.name for c in table.tableSchema.columns]
            existing.extend([
                c.propertyUrl.uri for c in table.tableSchema.columns if c.propertyUrl])
            col = make_column(col)
            if col.name in existing:
                raise ValueError('Duplicate column name: {0}'.format(col.name))
            if col.propertyUrl and col.propertyUrl.uri in existing:
                raise ValueError('Duplicate column property: {0}'.format(col.propertyUrl.uri))
            table.tableSchema.columns.append(make_column(col))
        self.auto_constraints()

    def add_foreign_key(self, foreign_t, foreign_c, primary_t, primary_c=None):
        foreign_c = self[foreign_t, foreign_c].name
        foreign_t = self[foreign_t]
        if not primary_c:
            primary_t = self[primary_t]
            primary_c = primary_t.tableSchema.primaryKey
        else:
            primary_c = self[primary_t, primary_c].name
            primary_t = self[primary_t]
        foreign_t.tableSchema.foreignKeys.append(ForeignKey.fromdict(dict(
            columnReference=foreign_c,
            reference=dict(
                resource=primary_t.url.string,
                columnReference=primary_c))))

    def auto_constraints(self, component=None):
        """
        Use CLDF reference properties to implicitely create foreign key constraints.

        :param component: A Table object or `None`.
        """
        if not component:
            for table in self.tables:
                self.auto_constraints(table)
            return

        if not component.tableSchema.primaryKey:
            idcol = component.get_column(term_uri('id'))
            if idcol:
                component.tableSchema.primaryKey = [idcol.name]

        self._auto_foreign_keys(component)

        try:
            table_type = self.get_tabletype(component)
        except ValueError:
            # New component is not a known CLDF term, so cannot add components
            # automatically. TODO: We might me able to infer some based on
            # `xxxReference` column properties?
            return

        # auto-add foreign keys targetting the new component:
        for table in self.tables:
            self._auto_foreign_keys(table, component=component, table_type=table_type)

    def _auto_foreign_keys(self, table, component=None, table_type=None):
        assert (component is None) == (table_type is None)
        for col in table.tableSchema.columns:
            if col.propertyUrl and col.propertyUrl.uri in TERMS.by_uri:
                ref_name = TERMS.by_uri[col.propertyUrl.uri].references
                if (component is None and not ref_name) or \
                        (component is not None and ref_name != table_type):
                    continue
                if any(fkey.columnReference == [col.name]
                       for fkey in table.tableSchema.foreignKeys):
                    continue
                if component is None:
                    # Let's see whether we have the component this column references:
                    try:
                        ref = self[ref_name]
                    except KeyError:
                        continue
                else:
                    ref = component
                idcol = ref.get_column(term_uri('id'))
                table.tableSchema.foreignKeys.append(ForeignKey.fromdict(dict(
                    columnReference=col.name,
                    reference=dict(
                        resource=ref.url.string,
                        columnReference=idcol.name if idcol is not None else 'ID'))))

    @property
    def bibpath(self):
        return self.directory.joinpath(self.properties.get('dc:source', 'sources.bib'))

    def validate(self, log=None, validators=None):
        validators = validators or []
        validators.extend(VALIDATORS)
        success = True
        default_tg = TableGroup.from_file(
            pkg_path('modules', '{0}{1}'.format(self.module, MD_SUFFIX)))
        for default_table in default_tg.tables:
            dtable_uri = default_table.common_props['dc:conformsTo']
            try:
                table = self[dtable_uri]
            except KeyError:
                log_or_raise('{0} requires {1}'.format(self.module, dtable_uri), log=log)
                success = False
                table = None

            if table:
                default_cols = {
                    c.propertyUrl.uri for c in default_table.tableSchema.columns
                    if c.required or c.common_props.get('dc:isRequiredBy')}
                cols = {
                    c.propertyUrl.uri for c in table.tableSchema.columns
                    if c.propertyUrl}
                table_uri = table.common_props['dc:conformsTo']
                for col in default_cols - cols:
                    log_or_raise('{0} requires column {1}'.format(table_uri, col), log=log)
                    success = False

        for table in self.tables:
            type_uri = table.common_props.get('dc:conformsTo')
            if type_uri:
                try:
                    TERMS.is_cldf_uri(type_uri)
                except ValueError:
                    success = False
                    log_or_raise('invalid CLDF URI: {0}'.format(type_uri), log=log)

            # FIXME: check whether table.common_props['dc:conformsTo'] is in validators!
            validators_ = []
            for col in table.tableSchema.columns:
                if col.propertyUrl:
                    col_uri = col.propertyUrl.uri
                    try:
                        TERMS.is_cldf_uri(col_uri)
                    except ValueError:
                        success = False
                        log_or_raise('invalid CLDF URI: {0}'.format(col_uri), log=log)
                for table_, col_, v_ in validators:
                    if (not table_ or table is self.get(table_)) and col is self.get((table, col_)):
                        validators_.append((col, v_))

            fname = Path(table.url.resolve(table._parent.base))
            if fname.exists():
                for fname, lineno, row in table.iterdicts(log=log, with_metadata=True):
                    for col, validate in validators_:
                        try:
                            validate(self, table, col, row)
                        except ValueError as e:
                            log_or_raise(
                                '{0}:{1}:{2} {3}'.format(fname.name, lineno, col.name, e),
                                log=log)
                            success = False
                if not table.check_primary_key(log=log):
                    success = False
            else:
                log_or_raise('{0} does not exist'.format(fname), log=log)
                success = False

        if not self.tablegroup.check_referential_integrity(log=log):
            success = False

        return success

    @property
    def directory(self):
        return self.tablegroup._fname.parent

    @property
    def module(self):
        return self.properties['dc:conformsTo'].split('#')[1]

    @property
    def version(self):
        return self.properties['dc:conformsTo'].split('/')[3]

    @classmethod
    def in_dir(cls, d, empty_tables=False):
        fname = Path(d)
        if not fname.exists():
            fname.mkdir()
        assert fname.is_dir()
        res = cls.from_metadata(fname)
        if empty_tables:
            del res.tables[:]
        return res

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

        comps = Counter()
        for table in tablegroup.tables:
            try:
                comps.update([Dataset.get_tabletype(table)])
            except ValueError:
                pass
        if comps and comps.most_common(1)[0][1] > 1:
            raise ValueError('{0}: duplicate components!'.format(fname))

        for mod in get_modules():
            if mod.match(tablegroup):
                return mod.cls(tablegroup)
        return cls(tablegroup)

    @classmethod
    def from_data(cls, fname):
        fname = Path(fname)
        colnames = next(iterrows(fname), [])
        if not colnames:
            raise ValueError('empty data file!')
        if cls is Dataset:
            try:
                cls = next(mod.cls for mod in get_modules() if mod.match(fname))
            except StopIteration:
                raise ValueError('{0} does not match a CLDF module spec'.format(fname))
            assert issubclass(cls, Dataset) and cls is not Dataset

        res = cls.from_metadata(fname.parent)
        required_cols = {
            c.name for c in res[res.primary_table].tableSchema.columns
            if c.required}
        if not required_cols.issubset(colnames):
            raise ValueError('missing columns: %r' % sorted(required_cols.difference(colnames)))
        return res

    def __repr__(self):
        return '<cldf:%s:%s at %s>' % (self.version, self.module, self.directory)

    def __getitem__(self, item):
        """
        Access to tables and columns.

        If a pair (table-spec, column-spec) is passed as `item`, a Column will be
        returned, otherwise `item` is assumed to be a table-spec.

        A table-spec may be
        - a CLDF ontology URI matching the dc:conformsTo property of a table
        - the local name of a CLDF ontology URI, where the complete URI matches the
          the dc:conformsTo property of a table
        - a filename matching the `url` property of a table

        A column-spec may be
        - a CLDF ontology URI matching the propertyUrl of a column
        - the local name of a CLDF ontology URI, where the complete URI matches the
          propertyUrl of a column
        - the name of a column
        """
        if isinstance(item, tuple):
            table, column = item
        else:
            table, column = item, None

        if not isinstance(table, Table):
            uri = term_uri(table, terms=TERMS.by_uri)
            for t in self.tables:
                if (uri and t.common_props.get('dc:conformsTo') == uri) \
                        or t.url.string == table:
                    break
            else:
                raise KeyError(table)
        else:
            t = table

        if not column:
            return t

        uri = term_uri(column, terms=TERMS.by_uri)
        for c in t.tableSchema.columns:
            if (c.propertyUrl and c.propertyUrl.uri == uri) or c.header == column:
                return c

        raise KeyError(column)

    def get(self, item, default=None):
        try:
            return self[item]
        except KeyError:
            return default

    def get_row(self, table, id_):
        id_col = self[table, TERMS['id']]
        for row in self[table]:
            if row[id_col.name] == id_:
                return row
        raise ValueError(id_)  # pragma: no cover

    @staticmethod
    def get_tabletype(table):
        if '#' in table.common_props.get('dc:conformsTo', ''):
            res = table.common_props['dc:conformsTo'].split('#')[1]
            if res in TERMS:
                return res
        raise ValueError("Type {:} of table {:} is not a valid term.".format(
            table.common_props.get('dc:conformsTo'),
            table.url))

    @property
    def primary_table(self):
        if self.tables:
            try:
                return self.get_tabletype(self.tables[0])
            except ValueError:
                return None

    def stats(self):
        res = []
        for table in self.tables:
            dctype = table.common_props.get('dc:conformsTo')
            if dctype.split('#')[1] in TERMS:
                dctype = TERMS[dctype.split('#')[1]].csvw_prop('name')
            res.append((
                table.url.string,
                dctype,
                table.common_props.get('dc:extent') or sum(1 for _ in table)))
        if self.sources:
            res.append((self.bibpath.name, 'Sources', len(self.sources)))
        return res

    def write_metadata(self, fname=None):
        return self.tablegroup.to_file(fname or self.tablegroup._fname)

    def write_sources(self):
        return self.sources.write(self.bibpath)

    def write(self, fname=None, **table_items):
        if self.sources and not self.properties.get('dc:source'):
            self.properties['dc:source'] = 'sources.bib'
        self.write_sources()
        for table_type, items in table_items.items():
            table = self[table_type]
            table.common_props['dc:extent'] = table.write(items)
        self.write_metadata(fname)


class Generic(Dataset):
    @property
    def primary_table(self):
        return None


class Wordlist(Dataset):
    @property
    def primary_table(self):
        return 'FormTable'

    def get_segments(self, row, table='FormTable'):
        col = self[table].get_column("http://cldf.clld.org/v1.0/terms.rdf#segments")
        sounds = row[col.name]
        if isinstance(sounds, string_types):
            # This may be the case when no morpheme boundaries are provided.
            sounds = [sounds]
        return list(chain(*[s.split() for s in sounds]))

    def get_subsequence(self, cognate, form=None):
        """
        Compute the subsequence of the morphemes of a form which is specified in a partial
        cognate assignment.

        :param partial_cognate:
        :return:
        """
        return resolve_slices(
            cognate,
            self,
            ('CognateTable', "http://cldf.clld.org/v1.0/terms.rdf#segmentSlice"),
            ('FormTable', "http://cldf.clld.org/v1.0/terms.rdf#segments"),
            'Form_ID',
            target_row=form)


class ParallelText(Dataset):
    @property
    def primary_table(self):
        return 'FormTable'

    def get_equivalent(self, functional_equivalent, form=None):
        return resolve_slices(
            functional_equivalent,
            self,
            ('FunctionalEquivalentTable',
             "http://cldf.clld.org/v1.0/terms.rdf#segmentSlice"),
            ('FormTable', "http://cldf.clld.org/v1.0/terms.rdf#segments"),
            'Form_ID',
            target_row=form)


class Dictionary(Dataset):
    @property
    def primary_table(self):
        return 'EntryTable'


class StructureDataset(Dataset):
    @property
    def primary_table(self):
        return 'ValueTable'
