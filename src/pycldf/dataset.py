import re
import sys
import json
import types
import shutil
import typing
import logging
import pathlib
import functools
import itertools
import collections
import collections.abc
import urllib.parse
import urllib.request

import attr
import csvw
from csvw.metadata import TableGroup, Table, Column, Link, Schema, is_url, URITemplate
from csvw import datatypes
from csvw.dsv import iterrows
from clldutils.path import git_describe, walk
from clldutils.misc import log_or_raise
from clldutils import jsonlib

from pycldf.sources import Sources
from pycldf.util import pkg_path, resolve_slices, DictTuple, sanitize_url, iter_uritemplates
from pycldf.terms import term_uri, Terms, TERMS, get_column_names, URL as TERMS_URL
from pycldf.validators import VALIDATORS
from pycldf import orm

__all__ = [
    'Dataset', 'Generic', 'Wordlist', 'ParallelText', 'Dictionary', 'StructureDataset',
    'TextCorpus', 'iter_datasets', 'sniff', 'SchemaError', 'ComponentWithValidation']

MD_SUFFIX = '-metadata.json'
ORM_CLASSES = {cls.component_name(): cls for cls in orm.Object.__subclasses__()}
TableType = typing.Union[str, Table]
ColType = typing.Union[str, Column]
PathType = typing.Union[str, pathlib.Path]
TableSpecType = typing.Union[str, Link, Table]
ColSPecType = typing.Union[str, Column]
SchemaObjectType = typing.Union[TableSpecType, typing.Tuple[TableSpecType, ColSPecType]]


class SchemaError(KeyError):
    pass


@attr.s
class Module:
    """
    Class representing a CLDF Module.

    .. seealso:: https://github.com/cldf/cldf/blob/master/README.md#cldf-modules
    """
    uri = attr.ib(validator=attr.validators.in_([t.uri for t in TERMS.classes.values()]))
    fname = attr.ib()
    cls = attr.ib(default=None)

    @property
    def id(self) -> str:
        """
        The local part of the term URI is interpreted as Module identifier.
        """
        return self.uri.split('#')[1]

    def match(self, thing) -> bool:
        if isinstance(thing, TableGroup):
            return thing.common_props.get('dc:conformsTo') == term_uri(self.id)
        if hasattr(thing, 'name'):
            return thing.name == self.fname
        return False


_modules = []


def get_modules() -> typing.List[Module]:
    """
    We read supported CLDF modules from the default metadata files distributed with `pycldf`.
    """
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


def make_column(spec: typing.Union[str, dict, Column]) -> Column:
    if isinstance(spec, str):
        if spec in TERMS.by_uri:
            return TERMS.by_uri[spec].to_column()
        return Column(name=spec, datatype='string')
    if isinstance(spec, dict):
        return Column.fromvalue(spec)
    if isinstance(spec, Column):
        return spec
    raise TypeError(spec)


class GitRepository:
    def __init__(self, url, clone=None, version=None, **dc):
        # We remove credentials from the URL immediately to make sure this isn't leaked into
        # CLDF metadata. Such credentials might be present in URLs read via gitpython from
        # remotes.
        self.url = sanitize_url(url)
        self.clone = clone
        self.version = version
        self.dc = dc

    def json_ld(self):
        res = collections.OrderedDict([
            ('rdf:about', self.url),
            ('rdf:type', 'prov:Entity'),
        ])
        if self.version:
            res['dc:created'] = self.version
        elif self.clone:
            res['dc:created'] = git_describe(self.clone)
        res.update({'dc:{0}'.format(k): self.dc[k] for k in sorted(self.dc)})
        return res


class Dataset:
    """
    API to access a CLDF dataset.
    """

    def __init__(self, tablegroup: csvw.TableGroup):
        """
        A :class:`~pycldf.dataset.Dataset` is initialized passing a `TableGroup`. The following \
        factory methods obviate the need to instantiate such a `TableGroup` instance yourself:

        - :meth:`~pycldf.dataset.Dataset.in_dir`
        - :meth:`~pycldf.dataset.Dataset.from_metadata`
        - :meth:`~pycldf.dataset.Dataset.from_data`
        """
        self.tablegroup = tablegroup
        self.auto_constraints()
        self._sources = None
        self._objects = collections.defaultdict(collections.OrderedDict)
        self._objects_by_pk = collections.defaultdict(collections.OrderedDict)

    @property
    def sources(self):
        # We load sources only the first time they are accessed, because for datasets like
        # Glottolog - with 40MB zipped BibTeX - this may take ~90secs.
        if self._sources is None:
            self._sources = Sources.from_file(self.bibpath)
        return self._sources

    @sources.setter
    def sources(self, obj):
        if not isinstance(obj, Sources):
            raise TypeError('Invalid type for Dataset.sources')
        self._sources = obj

    #
    # Factory methods to create `Dataset` instances.
    #
    @classmethod
    def in_dir(cls, d: PathType, empty_tables: bool = False) -> 'Dataset':
        """
        Create a :class:`~pycldf.dataset.Dataset` in a (possibly empty or even non-existing) \
        directory.

        The dataset will be initialized with the default metadata for the respective module.

        :rtype: :class:`~pycldf.dataset.Dataset`
        """
        fname = pathlib.Path(d)
        if not fname.exists():
            fname.mkdir()
        assert fname.is_dir()
        res = cls.from_metadata(fname)
        if empty_tables:
            del res.tables[:]
        return res

    @classmethod
    def from_metadata(cls, fname: PathType) -> 'Dataset':
        """
        Initialize a :class:`~pycldf.dataset.Dataset` with the metadata found at `fname`.

        :param fname: A URL (`str`) or a local path (`str` or `pathlib.Path`). \
        If `fname` points to a directory, the default metadata for the respective module will be \
        read.
        :rtype: :class:`~pycldf.dataset.Dataset`
        """
        if is_url(fname):
            tablegroup = TableGroup.from_url(fname)
        else:
            fname = pathlib.Path(fname)
            if fname.is_dir():
                name = '{0}{1}'.format(cls.__name__, MD_SUFFIX)
                tablegroup = TableGroup.from_file(pkg_path('modules', name))
                # adapt the path of the metadata file such that paths to tables are resolved
                # correctly:
                tablegroup._fname = fname.joinpath(name)
            else:
                tablegroup = TableGroup.from_file(fname)

        comps = collections.Counter()
        for table in tablegroup.tables:
            try:
                dt = Dataset.get_tabletype(table)
                if dt:
                    comps.update([dt])
            except ValueError:
                pass
        if comps and comps.most_common(1)[0][1] > 1:
            raise ValueError('{0}: duplicate components!'.format(fname))

        for mod in get_modules():
            if mod.match(tablegroup):
                return mod.cls(tablegroup)
        return cls(tablegroup)

    @classmethod
    def from_data(cls, fname: PathType) -> 'Dataset':
        """
        Initialize a :class:`~pycldf.dataset.Dataset` from a single CLDF data file.

        See https://github.com/cldf/cldf#metadata-free-conformance

        :rtype: :class:`~pycldf.dataset.Dataset`
        """
        fname = pathlib.Path(fname)
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

    #
    # Accessing dataset metadata
    #
    @property
    def directory(self) -> typing.Union[str, pathlib.Path]:
        """
        :return: The location of the metadata file. Either a local directory as `pathlib.Path` or \
        a URL as `str`.
        """
        return self.tablegroup._fname.parent if self.tablegroup._fname else self.tablegroup.base

    @property
    def filename(self) -> str:
        """
        :return: The name of the metadata file.
        """
        return self.tablegroup._fname.name if self.tablegroup._fname else \
            pathlib.Path(urllib.parse.urlparse(self.tablegroup.base).path).name

    @property
    def module(self) -> str:
        """
        :return: The name of the CLDF module of the dataset.
        """
        return self.properties['dc:conformsTo'].split('#')[1]

    @property
    def version(self) -> str:
        return self.properties['dc:conformsTo'].split('/')[3]

    def __repr__(self):
        return '<cldf:%s:%s at %s>' % (self.version, self.module, self.directory)

    @property
    def metadata_dict(self) -> dict:
        return self.tablegroup.asdict(omit_defaults=False)

    @property
    def properties(self) -> dict:
        """
        :return: Common properties of the CSVW TableGroup of the dataset.
        """
        return self.tablegroup.common_props

    @property
    def bibpath(self) -> typing.Union[str, pathlib.Path]:
        """
        :return: Location of the sources BibTeX file. Either a URL (`str`) or a local path \
        (`pathlib.Path`).
        """
        # Specifying "dc:source": "" means lookup the default location.
        if is_url(self.directory):
            return urllib.parse.urljoin(
                self.directory, self.properties.get('dc:source') or 'sources.bib')
        return self.directory.joinpath(self.properties.get('dc:source') or 'sources.bib')

    @property
    def bibname(self) -> str:
        """
        :return: Filename of the sources BibTeX file.
        """
        if is_url(self.bibpath):
            return pathlib.Path(urllib.parse.urlparse(self.bibpath).path).name
        return self.bibpath.name

    #
    # Accessing schema objects (components, tables, columns, foreign keys)
    #
    @property
    def tables(self) -> typing.List[Table]:
        """
        :return: All tables defined in the dataset.
        """
        return self.tablegroup.tables

    @property
    def components(self) -> typing.Dict[str, csvw.Table]:
        """
        :return: Mapping of component name to table objects as defined in the dataset.
        """
        res = collections.OrderedDict()
        for table in self.tables:
            comp = None
            try:
                comp = self.get_tabletype(table)
            except ValueError:
                pass
            if comp:
                res[comp] = table
        return res

    @staticmethod
    def get_tabletype(table) -> typing.Union[str, None]:
        if table.common_props.get('dc:conformsTo', '') is None:
            return None
        if '#' in table.common_props.get('dc:conformsTo', ''):
            res = table.common_props['dc:conformsTo'].split('#')[1]
            if res in TERMS:
                return res
        raise ValueError("Type {:} of table {:} is not a valid term.".format(
            table.common_props.get('dc:conformsTo'),
            table.url))

    @property
    def primary_table(self) -> typing.Union[str, None]:
        if self.tables:
            try:
                return self.get_tabletype(self.tables[0])
            except ValueError:
                return None

    def __getitem__(self, item: SchemaObjectType) -> typing.Union[csvw.Table, csvw.Column]:
        """
        Access to tables and columns.

        If a pair (table-spec, column-spec) is passed as ``item``, a :class:`csvw.Column` will be
        returned, otherwise ``item`` is assumed to be a table-spec, and a :class:`csvw.Table` is
        returned.

        A table-spec may be

        - a CLDF ontology URI matching the `dc:conformsTo` property of a table
        - the local name of a CLDF ontology URI, where the complete URI matches the \
          the `dc:conformsTo` property of a table
        - a filename matching the `url` property of a table.

        A column-spec may be

        - a CLDF ontology URI matching the `propertyUrl` of a column
        - the local name of a CLDF ontology URI, where the complete URI matches the \
          `propertyUrl` of a column
        - the name of a column.

        :param item: A schema object spec.
        :raises SchemaError: If no matching table or column is found.
        """
        if isinstance(item, tuple):
            table, column = item
        else:
            table, column = item, None

        if isinstance(table, Link):
            table = table.string

        if not isinstance(table, Table):
            uri = term_uri(table, terms=TERMS.by_uri)
            for t in self.tables:
                if (uri and t.common_props.get('dc:conformsTo') == uri) \
                        or t.url.string == table:
                    break
            else:
                raise SchemaError('Dataset has no table "{}"'.format(table))
        else:
            if any(table is tt for tt in self.tables):
                t = table
            else:
                raise SchemaError('Dataset has no table "{}"'.format(table))

        if not column:
            return t

        if isinstance(column, Column):
            if any(column is c for c in t.tableSchema.columns):
                return column
            else:
                raise SchemaError('Dataset has no column "{}" in table "{}"'.format(
                    column.name, t.url))

        uri = term_uri(column, terms=TERMS.by_uri)
        for c in t.tableSchema.columns:
            if (c.propertyUrl and c.propertyUrl.uri == uri) or c.header == column:
                return c

        raise SchemaError('Dataset has no column "{}" in table "{}"'.format(column, t.url))

    def __delitem__(self, item: SchemaObjectType):
        """
        Remove a table or column from the datasets' schema.

        :param item: See  :meth:`~pycldf.dataset.Dataset.__getitem__`
        """
        thing = self[item]
        if isinstance(thing, Column):
            self.remove_columns(self[item[0]], thing)
        else:
            self.remove_table(thing)

    def __contains__(self, item: SchemaObjectType) -> bool:
        """
        Check whether a dataset specifies a table or column.

        :param item: See  :meth:`~pycldf.dataset.Dataset.__getitem__`
        """
        return bool(self.get(item))

    def get(self,
            item: SchemaObjectType,
            default=None) -> typing.Union[csvw.Table, csvw.Column, None]:
        """
        Acts like `dict.get`.

        :param item: See  :meth:`~pycldf.dataset.Dataset.__getitem__`
        """
        try:
            return self[item]
        except SchemaError:
            return default

    def get_foreign_key_reference(self, table: TableType, column: ColType) \
            -> typing.Union[typing.Tuple[csvw.Table, csvw.Column], None]:
        """
        Retrieve the reference of a foreign key constraint for the specified column.

        :param table: Source table, specified by filename, component name or as `Table` instance.
        :param column: Source column, specified by column name, CLDF term or as `Column` instance.
        :return: A pair (`Table`, `Column`) specifying the reference column - or `None`.
        """
        table = self[table]
        column = self[table, column]

        for fk in table.tableSchema.foreignKeys:
            if len(fk.columnReference) == 1 and fk.columnReference[0] == column.name:
                return self[fk.reference.resource], \
                    self[fk.reference.resource, fk.reference.columnReference[0]]

    @property
    def column_names(self) -> types.SimpleNamespace:
        """
        In-direction layer, mapping ontology terms to local column names (or `None`).

        Note that this property is computed each time it is accessed (because the dataset
        schema may have changed). So when accessing a dataset for reading only, calling code
        should use `readonly_column_names`.

        :return: an `types.SimpleNamespace` object, with attributes `<object>s` for each component \
        `<Object>Table` defined in the ontology. Each such attribute evaluates to `None` if the \
        dataset does not contain the component. Otherwise, it's an `types.SimpleNamespace` object \
        mapping each property defined in the ontology to `None` - if no such column is specified \
        in the component - and the local column name if it is.
        """
        return get_column_names(self)

    @functools.cached_property
    def readonly_column_names(self) -> types.SimpleNamespace:
        """
        :return: `types.SimpleNamespace` with component names as attributes.
        """
        return get_column_names(self, use_component_names=True, with_multiplicity=True)

    #
    # Editing dataset metadata or schema
    #
    def add_provenance(self, **kw):
        """
        Add metadata about the dataset's provenance.

        :param kw: Key-value pairs, where keys are local names of properties in the PROV ontology \
        for describing entities (see https://www.w3.org/TR/2013/REC-prov-o-20130430/#Entity).
        """
        def to_json(obj):
            if isinstance(obj, GitRepository):
                return obj.json_ld()
            return obj

        for k, v in kw.items():
            if not k.startswith('prov:'):
                k = 'prov:{0}'.format(k)
            if isinstance(v, (tuple, list)):
                v = [to_json(vv) for vv in v]
            else:
                v = to_json(v)
            if k in self.tablegroup.common_props:
                old = self.tablegroup.common_props.pop(k)
                if not isinstance(old, list):
                    old = [old]
                for vv in (v if isinstance(v, list) else [v]):
                    if vv not in old:
                        old.append(vv)
                v = old
            self.tablegroup.common_props[k] = v

    def add_table(self, url: str, *cols, **kw) -> csvw.Table:
        """
        Add a table description to the Dataset.

        :param url: The url property of the table.
        :param cols: Column specifications; anything accepted by :func:`pycldf.dataset.make_column`.
        :param kw: Recognized keywords:
            - `primaryKey`: specify the column(s) constituting the primary key of the table.
        :return: The new table.
        """
        t = self.add_component({"url": url, "tableSchema": {"columns": []}}, *cols)
        if 'primaryKey' in kw:
            t.tableSchema.primaryKey = attr.fields_dict(Schema)['primaryKey'].converter(
                kw.pop('primaryKey'))
        return t

    def remove_table(self, table: TableType):
        """
        Removes the table specified by `table` from the dataset.
        """
        table = self[table]

        # First remove foreign keys:
        for t in self.tables:
            t.tableSchema.foreignKeys = [
                fk for fk in t.tableSchema.foreignKeys if fk.reference.resource != table.url]

        # Now remove the table:
        self.tablegroup.tables = [t for t in self.tablegroup.tables if t.url != table.url]

    def add_component(self, component: typing.Union[str, dict], *cols, **kw) -> csvw.Table:
        """
        Add a CLDF component to a dataset.

        .. seealso:: https://github.com/cldf/cldf/blob/master/README.md#cldf-components

        :param component: A component specified by name or as `dict` representing the JSON \
        description of the component.
        """
        if isinstance(component, str):
            component = jsonlib.load(pkg_path('components', '{0}{1}'.format(component, MD_SUFFIX)))
        if isinstance(component, dict):
            component = Table.fromvalue(component)
        assert isinstance(component, Table)

        if kw.get('url'):
            component.url = Link(kw['url'])

        for other_table in self.tables:
            if other_table.url == component.url:
                raise ValueError('tables must have distinct url properties')

        try:
            table_type = self.get_tabletype(component)
        except ValueError:
            table_type = None
        if table_type:
            for other_table in self.tables:
                try:
                    other_table_type = self.get_tabletype(other_table)
                except ValueError:  # pragma: no cover
                    continue
                if other_table_type == table_type:
                    raise ValueError('components must not be added twice')
        self.tables.append(component)
        self.add_columns(component, *cols)

        component._parent = self.tablegroup
        self.auto_constraints(component)
        return component

    def add_columns(self, table: TableType, *cols) -> None:
        """
        Add columns specified by `cols` to the table specified by `table`.
        """
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
            table.tableSchema.columns.append(col)
        self.auto_constraints()

    def remove_columns(self, table: TableType, *cols):
        """
        Remove `cols` from `table`'s schema.

        .. note:: Foreign keys pointing to any of the removed columns are removed as well.
        """
        table = self[table]
        cols = [str(self[table, col]) for col in cols]

        # First remove foreign keys:
        for t in self.tables:
            t.tableSchema.foreignKeys = [
                fk for fk in t.tableSchema.foreignKeys
                if (fk.reference.resource != table.url or  # noqa: W504
                    (not set(str(c) for c in fk.reference.columnReference).intersection(cols)))]

        # Remove primary key constraints:
        if table.tableSchema.primaryKey:
            if set(str(c) for c in table.tableSchema.primaryKey).intersection(cols):
                table.tableSchema.primaryKey = None

        table.tableSchema.columns = [c for c in table.tableSchema.columns if str(c) not in cols]

    def rename_column(self, table: TableType, col: ColType, name: str):
        """
        Assign a new `name` to an existing column, cascading this change to foreign keys.

        This functionality can be used to change the names of columns added automatically by
        :meth:`Dataset.add_component`
        """
        table = self[table]
        col = self[table, col]

        for obj, prop, tmpl in iter_uritemplates(table):
            if col.name in tmpl.variable_names:
                old = str(tmpl)
                new = re.sub(
                    r'{([^}]+)}',
                    lambda m: '{' + re.sub(re.escape(col.name), name, m.groups()[0]) + '}',
                    old)
                if old != new:
                    setattr(obj, prop, URITemplate(new))

        if col.name in table.tableSchema.primaryKey:
            table.tableSchema.primaryKey = [
                name if n == col.name else n for n in table.tableSchema.primaryKey]

        for t in self.tables:
            for fk in t.tableSchema.foreignKeys:
                if fk.reference.resource == table.url and col.name in fk.reference.columnReference:
                    fk.reference.columnReference = [
                        name if n == col.name else n for n in fk.reference.columnReference]

                if t.url == table.url and col.name in fk.columnReference:
                    # We also need to check the columnReference
                    fk.columnReference = [name if n == col.name else n for n in fk.columnReference]

        col.name = name

    def add_foreign_key(
            self,
            foreign_t: TableType,
            foreign_c: ColType,
            primary_t: TableType,
            primary_c: typing.Optional[ColType] = None):
        """
        Add a foreign key constraint.

        ..note:: Composite keys are not supported yet.

        :param foreign_t: Table reference for the linking table.
        :param foreign_c: Column reference for the link.
        :param primary_t: Table reference for the linked table.
        :param primary_c: Column reference for the linked column - or `None`, in which case the \
        primary key of the linked table is assumed.
        """
        if isinstance(foreign_c, (tuple, list)) or isinstance(primary_c, (tuple, list)):
            raise NotImplementedError('composite keys are not supported')

        foreign_t = self[foreign_t]
        primary_t = self[primary_t]
        if not primary_c:
            primary_c = primary_t.tableSchema.primaryKey
        else:
            primary_c = self[primary_t, primary_c].name
        foreign_t.add_foreign_key(self[foreign_t, foreign_c].name, primary_t.url.string, primary_c)

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
            table_type = None

        if table_type is None:
            # New component is not a known CLDF term, so cannot add components
            # automatically. TODO: We might me able to infer some based on
            # `xxxReference` column properties?
            return

        # auto-add foreign keys targeting the new component:
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
                table.add_foreign_key(
                    col.name, ref.url.string, idcol.name if idcol is not None else 'ID')

    #
    # Add data
    #
    def add_sources(self, *sources, **kw):
        """
        Add sources to the dataset.

        :param sources: Anything accepted by :meth:`pycldf.sources.Sources.add`.
        """
        self.sources.add(*sources, **kw)

    #
    # Methods to read data
    #
    def iter_rows(self, table: TableType, *cols) -> typing.Iterator[dict]:
        """
        Iterate rows in a table, resolving CLDF property names to local column names.

        :param table: Table name.
        :param cols: List of CLDF property terms which must be resolved in resulting `dict` s. \
        I.e. the row dicts will be augmented with copies of the values keyed with CLDF property \
        terms.
        """
        cmap = {self[table, col].name: col for col in cols}
        for item in self[table]:
            for k, v in cmap.items():
                # Add CLDF properties as aliases for the corresponding values:
                item[v] = item[k]
            yield item

    def get_row(self, table: TableType, id_) -> dict:
        """
        Retrieve a row specified by table and CLDF id.

        :raises ValueError: If no matching row is found.
        """
        id_col = self[table, TERMS['id']]
        for row in self[table]:
            if row[id_col.name] == id_:
                return row
        raise ValueError(id_)  # pragma: no cover

    def get_row_url(self, table: TableType, row) -> typing.Union[str, None]:
        """
        Get a URL associated with a row. Tables can specify associated row URLs by

        - listing **one** column with datatype `anyURI` or
        - specfying a `valueUrl` property for their ID column.

        For rows representing objects in web applications, this may be the objects URL. For
        rows representing media files, it may be a URL locating the file on a media server.

        :param table: Table specified in a way that `__getitem__` understands.
        :param row: A row specified by ID or as `dict` as returned when iterating over a table.
        :return: a `str` representing a URL or `None`.
        """
        row = row if isinstance(row, dict) else self.get_row(table, row)
        id_col = None
        for col in self[table].tableSchema.columns:
            if col.datatype and col.datatype.base == datatypes.anyURI.__name__:
                # If one of the columns in the table explicitly spacifies anyURI as datatype, we
                # return the value of this column.
                return row[col.name]
            if str(col.propertyUrl) == 'http://cldf.clld.org/v1.0/terms.rdf#id':
                # Otherwise we fall back to looking up the `valueUrl` property on the ID column.
                id_col = col
        assert id_col, 'no ID column found in table {}'.format(table)
        if id_col.valueUrl:
            return id_col.valueUrl.expand(**row)

    def objects(self, table: str, cls: typing.Optional[typing.Type] = None) -> DictTuple:
        """
        Read data of a CLDF component as :class:`pycldf.orm.Object` instances.

        :param table: table to read, specified as component name.
        :param cls: :class:`pycldf.orm.Object` subclass to instantiate objects with.
        :return:
        """
        cls = cls or ORM_CLASSES[table]

        # ORM usage is read-only, so we can cache the objects.
        if table not in self._objects:
            for item in self[table]:
                item = cls(self, item)
                self._objects[table][item.id] = item
                if item.pk:
                    self._objects_by_pk[table][item.pk] = item

        return DictTuple(self._objects[table].values())

    def get_object(self, table, id_, cls=None, pk=False) -> orm.Object:
        """
        Get a row of a component as :class:`pycldf.orm.Object` instance.
        """
        if table not in self._objects:
            self.objects(table, cls=cls)
        return self._objects[table][id_] if not pk else self._objects_by_pk[table][id_]

    #
    # Methods for writing (meta)data to files:
    #
    def write_metadata(
            self, fname: typing.Optional[typing.Union[str, pathlib.Path]] = None) -> pathlib.Path:
        """
        Write the CLDF metadata to a JSON file.

        :fname: Path of a file to write to, or `None` to use the default name and write to \
        :meth:`~pycldf.dataset.Dataset.directory`.
        """
        return self.tablegroup.to_file(fname or self.tablegroup._fname)

    def write_sources(self, zipped: bool = False) -> typing.Union[None, pathlib.Path]:
        """
        Write the sources BibTeX file to :meth:`~pycldf.dataset.Dataset.bibpath`

        :return: `None`, if no BibTeX file was written (because no source items were added), \
        `pathlib.Path` of the written BibTeX file otherwise. Note that this path does not need \
        to exist, because the content may have been added to a zip archive.
        """
        return self.sources.write(self.bibpath, zipped=zipped)

    def write(self,
              fname: typing.Optional[pathlib.Path] = None,
              zipped: typing.Optional[typing.Iterable] = None,
              **table_items: typing.List[dict]) -> pathlib.Path:
        """
        Write metadata, sources and data. Metadata will be written to `fname` (as interpreted in
        :meth:`pycldf.dataset.Dataset.write_metadata`); data files will be written to the file
        specified by `csvw.Table.url` of the corresponding table, interpreted as path relative
        to :meth:`~pycldf.dataset.Dataset.directory`.

        :param zipped: Iterable listing keys of `table_items` for which the table file should \
        be zipped.
        :param table_items: Mapping of table specifications to lists of row dicts.
        :return: Path of the CLDF metadata file as written to disk.
        """
        zipped = zipped or set()
        if self.sources and not self.properties.get('dc:source'):
            self.properties['dc:source'] = 'sources.bib'
        self.write_sources(
            zipped=self.properties.get('dc:source') in zipped or ('Source' in zipped))
        for table_type, items in table_items.items():
            table = self[table_type]
            table.common_props['dc:extent'] = table.write(items, _zipped=table_type in zipped)
        return self.write_metadata(fname)

    def copy(self, dest: typing.Union[str, pathlib.Path], mdname: str = None) -> pathlib.Path:
        """
        Copy metadata, data and sources to files in `dest`.

        :param dest: Destination directory.
        :param mdname: Name of the new metadata file.
        :return: Path of the new CLDF metadata file.

        This can be used together with :func:`iter_datasets` to extract CLDF data from their
        curation context, e.g. cldfbench-curated datasets from the repository they are curated in.

        .. code-block:: python

            >>> from pycldf import iter_datasets
            >>> for ds in iter_datasets('tests/data'):
            ...     if 'with_examples' in ds.directory.name:
            ...         ds.copy('some_directory', mdname='md.json')
        """
        dest = pathlib.Path(dest)
        if not dest.exists():
            dest.mkdir(parents=True)

        from_url = is_url(self.tablegroup.base)
        ds = Dataset.from_metadata(self.tablegroup.base if from_url else self.tablegroup._fname)

        _getter = urllib.request.urlretrieve if from_url else shutil.copy
        try:
            _getter(self.bibpath, dest / self.bibname)
            ds.properties['dc:source'] = self.bibname
        except:  # pragma: no cover # noqa
            # Sources are optional
            pass

        for table in ds.tables:
            fname = table.url.resolve(table.base)
            name = pathlib.Path(urllib.parse.urlparse(fname).path).name if from_url else fname.name
            _getter(fname, dest / name)
            table.url = Link(name)

            for fk in table.tableSchema.foreignKeys:
                fk.reference.resource = Link(pathlib.Path(fk.reference.resource.string).name)
        mdpath = dest.joinpath(
            mdname or  # noqa: W504
            (self.tablegroup.base.split('/')[-1] if from_url else self.tablegroup._fname.name))
        if from_url:
            del ds.tablegroup.at_props['base']  # pragma: no cover
        ds.write_metadata(fname=mdpath)
        return mdpath

    #
    # Reporting
    #
    def validate(
            self,
            log: logging.Logger = None,
            validators: typing.List[typing.Tuple[str, str, callable]] = None,
            ontology_path=None) -> bool:
        """
        Validate schema and data of a `Dataset`:

        - Make sure the schema follows the CLDF specification and
        - make sure the data is consistent with the schema.

        :param log: a `logging.Logger` to write ERRORs and WARNINGs to. If `None`, an exception \
        will be raised at the first problem.
        :param validators: Custom validation rules, i.e. triples \
        (tablespec, columnspec, attrs validator)
        :raises ValueError: if a validation error is encountered (and `log` is `None`).
        :return: Flag signaling whether schema and data are valid.
        """
        from pycldf.media import MediaTable
        from pycldf.trees import TreeTable

        assert MediaTable and TreeTable

        terms = Terms(ontology_path) or TERMS
        validators = validators or []
        validators.extend(VALIDATORS)
        success = True
        default_tg = TableGroup.from_file(
            pkg_path('modules', '{0}{1}'.format(self.module, MD_SUFFIX)))
        #
        # Make sure, all required tables and columns are present and consistent.
        #
        for default_table in default_tg.tables:
            dtable_uri = default_table.common_props['dc:conformsTo']
            try:
                table = self[dtable_uri]
            except KeyError:
                log_or_raise('{0} requires {1}'.format(self.module, dtable_uri), log=log)
                success = False
                table = None

            if table:
                default_cols = {c.propertyUrl.uri: c for c in default_table.tableSchema.columns}
                required_default_cols = {
                    c.propertyUrl.uri for c in default_table.tableSchema.columns
                    if c.required or c.common_props.get('dc:isRequiredBy')}
                cols = {
                    c.propertyUrl.uri: c for c in table.tableSchema.columns
                    if c.propertyUrl}
                table_uri = table.common_props['dc:conformsTo']
                for col in required_default_cols - set(cols.keys()):
                    log_or_raise('{0} requires column {1}'.format(table_uri, col), log=log)
                    success = False
                for uri, col in cols.items():
                    default = default_cols.get(uri)
                    if default:
                        cardinality = default.common_props.get('dc:extent')
                        if not cardinality:
                            cardinality = terms.by_uri[uri].cardinality
                        if (cardinality == 'multivalued' and not col.separator) or \
                                (cardinality == 'singlevalued' and col.separator):
                            log_or_raise('{} {} must be {}'.format(
                                table_uri, uri, cardinality), log=log)

        for table in self.tables:
            vars = set(col.name for col in table.tableSchema.columns)
            for obj, prop, tmpl in iter_uritemplates(table):
                if not {n for n in tmpl.variable_names if not n.startswith('_')}.issubset(vars):
                    if log:
                        log.warning('Unknown variables in URI template: {}:{}:{}'.format(
                            obj, prop, tmpl))

            type_uri = table.common_props.get('dc:conformsTo')
            if type_uri:
                try:
                    terms.is_cldf_uri(type_uri)
                except ValueError:
                    success = False
                    log_or_raise('invalid CLDF URI: {0}'.format(type_uri), log=log)

            if not table.tableSchema.primaryKey:
                if log:
                    log.warning('Table without primary key: {0} - {1}'.format(
                        table.url,
                        'This may cause problems with "cldf createdb"'))
            elif len(table.tableSchema.primaryKey) > 1:
                if log:
                    log.warning('Table with composite primary key: {0} - {1}'.format(
                        table.url,
                        'This may cause problems with "cldf createdb"'))

            # FIXME: check whether table.common_props['dc:conformsTo'] is in validators!
            validators_, propertyUrls, colnames = [], set(), set()
            for col in table.tableSchema.columns:
                if col.header in colnames:  # pragma: no cover
                    log_or_raise(
                        'Duplicate column name in table schema: {} {}'.format(
                            table.url, col.header),
                        log=log)
                colnames.add(col.header)
                if col.propertyUrl:
                    col_uri = col.propertyUrl.uri
                    try:
                        terms.is_cldf_uri(col_uri)
                        if col_uri in propertyUrls:  # pragma: no cover
                            log_or_raise(
                                'Duplicate CLDF property in table schema: {} {}'.format(
                                    table.url, col_uri),
                                log=log)
                        propertyUrls.add(col_uri)
                    except ValueError:
                        success = False
                        log_or_raise('invalid CLDF URI: {0}'.format(col_uri), log=log)
                for table_, col_, v_ in validators:
                    if (not table_ or table is self.get(table_)) and col is self.get((table, col_)):
                        validators_.append((col, v_))

            fname = pathlib.Path(table.url.resolve(table._parent.base))
            fexists = fname.exists()
            if (not fexists) and fname.parent.joinpath('{}.zip'.format(fname.name)).exists():
                if log:
                    log.info('Reading data from zipped table: {}.zip'.format(fname))
                fexists = True  # csvw already handles this case, no need to adapt paths.
            if fexists:
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

        for cls in ComponentWithValidation.__subclasses__():
            if cls.__name__ in self:
                success = cls(self).validate(success, log=log)

        return success

    def stats(self, exact=False) -> typing.List[typing.Tuple[str, str, int]]:
        """
        Compute summary statistics for the dataset.

        :return: List of triples (table, type, rowcount).
        """
        res = []
        for table in self.tables:
            dctype = table.common_props.get('dc:conformsTo')
            if dctype and '#' in dctype and dctype.split('#')[1] in TERMS:
                dctype = TERMS[dctype.split('#')[1]].csvw_prop('name')
            res.append((
                table.url.string,
                dctype,
                sum(1 for _ in table) if (exact or 'dc:extent' not in table.common_props)
                else int(table.common_props.get('dc:extent'))))
        if self.sources:
            res.append((self.bibname, 'Sources', len(self.sources)))
        return res


class Generic(Dataset):
    """
    Generic datasets have no primary table.

    .. seealso:: `<https://github.com/cldf/cldf/tree/master/modules/Generic>`_
    """
    @property
    def primary_table(self):
        return None


class Wordlist(Dataset):
    """
    Wordlists have support for segment slice notation.

    .. seealso:: `<https://github.com/cldf/cldf/tree/master/modules/Wordlist>`_
    """
    @property
    def primary_table(self):
        return 'FormTable'

    def get_segments(self, row, table='FormTable') -> typing.List[str]:
        col = self[table].get_column("http://cldf.clld.org/v1.0/terms.rdf#segments")
        sounds = row[col.name]
        if isinstance(sounds, str):
            # This may be the case when no morpheme boundaries are provided.
            sounds = [sounds]
        return list(itertools.chain(*[s.split() for s in sounds]))

    def get_subsequence(self, cognate: dict, form=None) -> typing.List[str]:
        """
        Compute the subsequence of the morphemes of a form which is specified in a partial
        cognate assignment.

        :param cognate: A `dict` holding the data of a row from a `CognateTable`.
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
    """
    Parameters in StructureDataset are often called "features".

    .. seealso:: `<https://github.com/cldf/cldf/tree/master/modules/StructureDataset>`_
    """
    @property
    def primary_table(self):
        return 'ValueTable'

    @functools.cached_property
    def features(self):
        """
        Just an alias for the parameters.
        """
        return self.objects('ParameterTable')


class TextCorpus(Dataset):
    """
    In a `TextCorpus`, contributions and examples have specialized roles:

    - Contributions are understood as individual texts of the corpus.
    - Examples are interpreted as the sentences of the corpus.
    - Alternative translations are provided by linking "light-weight" examples to "full", main
      examples.
    - The order of sentences may be defined using a `position` property.

    .. seealso:: `<https://github.com/cldf/cldf/tree/master/modules/TextCorpus>`_

    .. code-block:: python

        >>> crp = TextCorpus.from_metadata('tests/data/textcorpus/metadata.json')
        >>> crp.texts[0].sentences[0].cldf.primaryText
        'first line'
        >>> crp.texts[0].sentences[0].alternative_translations
        [<pycldf.orm.Example id="e2-alt">]
    """
    @property
    def primary_table(self):
        return 'ExampleTable'

    @functools.cached_property
    def texts(self) -> typing.Union[None, DictTuple]:
        # Some syntactic sugar to access the ORM data in a concise and meaningful way.
        if 'ContributionTable' in self:
            return self.objects('ContributionTable')

    def get_text(self, tid):
        if 'ContributionTable' in self:
            return self.get_object('ContributionTable', tid)

    @property
    def sentences(self) -> typing.List[orm.Example]:
        res = list(self.objects('ExampleTable'))
        if ('ExampleTable', 'exampleReference') in self:
            # Filter out alternative translations!
            res = [e for e in res if not e.cldf.exampleReference]
        if ('ExampleTable', 'position') in self:
            return sorted(res, key=lambda o: o.cldf.position)
        return res  # pragma: no cover


class ComponentWithValidation:
    def __init__(self, ds: Dataset):
        self.ds = ds
        self.component = self.__class__.__name__
        self.table = ds[self.component]

    def validate(self, success: bool = True, log: logging.Logger = None) -> bool:
        return success  # pragma: no cover


def sniff(p: pathlib.Path) -> bool:
    """
    Determine whether a file contains CLDF metadata.

    :param p: `pathlib.Path` object for an existing file.
    :return: `True` if the file contains CLDF metadata, `False` otherwise.
    """
    if not p.is_file():  # pragma: no cover
        return False
    try:
        with p.open('rb') as fp:
            c = fp.read(10)
            try:
                c = c.decode('utf8').strip()
            except UnicodeDecodeError:
                return False
            if not c.startswith('{'):
                return False
    except (FileNotFoundError, OSError):  # pragma: no cover
        return False
    try:
        d = jsonlib.load(p)
    except json.decoder.JSONDecodeError:
        return False
    return d.get('dc:conformsTo', '').startswith(TERMS_URL)


def iter_datasets(d: pathlib.Path) -> typing.Iterator[Dataset]:
    """
    Discover CLDF datasets - by identifying metadata files - in a directory.

    :param d: directory
    :return: generator of `Dataset` instances.
    """
    for p in walk(d, mode='files'):
        if sniff(p):
            try:
                yield Dataset.from_metadata(p)
            except ValueError as e:
                logging.getLogger(__name__).warning(
                    "Reading {} failed: {}".format(p, e))
