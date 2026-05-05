"""
An implementation of a CLDF dataset object.
"""
import re
import types
from typing import Union, Optional, Type, Any
import logging
import pathlib
import functools
import itertools
import collections
from collections.abc import Generator, Iterable
import urllib.parse
import urllib.request

import csvw
from csvw.metadata import TableGroup, Table, Column, Link, is_url, URITemplate
from csvw import datatypes
from csvw.dsv import iterrows
from clldutils.path import walk

from pycldf.module import get_module_impl, get_modules
from pycldf.sources import Sources, Source
from pycldf.util import (
    pkg_path, DictTuple, iter_uritemplates, MD_SUFFIX, GitRepository, copy_dataset)
from pycldf.sliceutil import multislice_with_split
from pycldf.fileutil import PathType
from pycldf.schemautil import ColSpecType, make_column, make_table, TableType, ColType
from pycldf.constraints import add_foreign_key, add_auto_constraints
from pycldf.terms import term_uri, Terms, TERMS, get_column_names, sniff
from pycldf import validators as validation
from pycldf.stats import get_table_stats
from pycldf import orm

assert get_modules  # For backwards compatibility with cldfbench.

__all__ = [
    'Dataset', 'Generic', 'Wordlist', 'ParallelText', 'Dictionary', 'StructureDataset',
    'TextCorpus', 'iter_datasets', 'sniff', 'SchemaError']

ORM_CLASSES = {cls.component_name(): cls for cls in orm.Object.__subclasses__()}
TableSpecType = Union[str, Link, Table]
SchemaObjectType = Union[TableSpecType, tuple[TableSpecType, ColType]]
ODict = collections.OrderedDict
RowType = ODict[str, Any]


class SchemaError(KeyError):
    """Schema objects can be accessed using `Dataset.__getitem__`."""


class Dataset:  # pylint: disable=too-many-public-methods
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
        self.tablegroup: csvw.TableGroup = tablegroup
        self.auto_constraints()
        self._sources = None
        self._objects = collections.defaultdict(collections.OrderedDict)
        self._objects_by_pk = collections.defaultdict(collections.OrderedDict)
        self._cached_rows = {}

    @property
    def sources(self) -> Sources:
        """The sources."""
        # We load sources only the first time they are accessed, because for datasets like
        # Glottolog - with 40MB zipped BibTeX - this may take ~90secs.
        if self._sources is None:
            self._sources = Sources.from_file(self.bibpath)
        return self._sources

    @sources.setter
    def sources(self, obj: Sources):
        if not isinstance(obj, Sources):
            raise TypeError('Invalid type for Dataset.sources')
        self._sources = obj

    # Factory methods to create `Dataset` instances. -----------------------------------------------
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
                name = f'{cls.__name__}{MD_SUFFIX}'
                tablegroup = TableGroup.from_file(pkg_path('modules', name))
                # adapt the path of the metadata file such that paths to tables are resolved
                # correctly:
                tablegroup._fname = fname.joinpath(name)  # pylint: disable=W0212
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
            raise ValueError(f'{fname}: duplicate components!')

        impl = get_module_impl(Dataset, tablegroup)
        if impl:
            return impl(tablegroup)
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
            impl = get_module_impl(Dataset, fname.name)
            if impl is None:
                raise ValueError(f'{fname} does not match a CLDF module spec')
            res = impl.from_metadata(fname.parent)
        else:
            res = cls.from_metadata(fname.parent)
        required_cols = {
            c.name for c in res[res.primary_table].tableSchema.columns
            if c.required}
        if not required_cols.issubset(colnames):
            raise ValueError(f'missing columns: {sorted(required_cols.difference(colnames))}')
        return res

    # Accessing dataset metadata -------------------------------------------------------------------
    @property
    def directory(self) -> PathType:
        """
        :return: The location of the metadata file. Either a local directory as `pathlib.Path` or \
        a URL as `str`.
        """
        if self.tablegroup._fname:  # pylint: disable=W0212
            return self.tablegroup._fname.parent  # pylint: disable=W0212
        return self.tablegroup.base

    @property
    def filename(self) -> str:
        """
        :return: The name of the metadata file.
        """
        if self.tablegroup._fname:  # pylint: disable=W0212
            return self.tablegroup._fname.name  # pylint: disable=W0212
        return pathlib.Path(urllib.parse.urlparse(self.tablegroup.base).path).name

    @property
    def module(self) -> str:
        """
        :return: The name of the CLDF module of the dataset.
        """
        return self.properties['dc:conformsTo'].split('#')[1]

    @property
    def version(self) -> str:
        """The CLDF version."""
        return self.properties['dc:conformsTo'].split('/')[3]

    def __repr__(self) -> str:
        return f'<cldf:{self.version}:{self.module} at {self.directory}>'

    @property
    def metadata_dict(self) -> dict:
        """The TableGroup instance as dict."""
        return self.tablegroup.asdict(omit_defaults=False)

    @property
    def properties(self) -> dict:
        """
        :return: Common properties of the CSVW TableGroup of the dataset.
        """
        return self.tablegroup.common_props

    @property
    def bibpath(self) -> PathType:
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

    # Accessing schema objects (components, tables, columns, foreign keys) -------------------------
    @property
    def tables(self) -> list[Table]:
        """
        :return: All tables defined in the dataset.
        """
        return self.tablegroup.tables

    @property
    def components(self) -> collections.OrderedDict[str, csvw.Table]:
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
    def get_tabletype(table) -> Optional[str]:
        """Return the table type, aka component name, of the table."""
        if table.common_props.get('dc:conformsTo', '') is None:
            return None
        if '#' in table.common_props.get('dc:conformsTo', ''):
            res = table.common_props['dc:conformsTo'].split('#')[1]
            if res in TERMS:
                return res
        raise ValueError(
            f"Type {table.common_props.get('dc:conformsTo')} of table {table.url} is invalid.")

    @property
    def primary_table(self) -> Optional[str]:
        """Returns the primary table for the dataset."""
        if self.tables:
            try:
                return self.get_tabletype(self.tables[0])
            except ValueError:
                pass
        return None

    def __getitem__(self, item: SchemaObjectType) -> Union[csvw.Table, csvw.Column]:
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

        t = self._get_table(table)
        if not column:
            return t

        if isinstance(column, Column):
            if any(column is c for c in t.tableSchema.columns):
                return column
            raise SchemaError(f'Dataset has no column "{column.name}" in table "{t.url}"')

        uri = term_uri(column, terms=TERMS.by_uri)
        for c in t.tableSchema.columns:
            if ((c.propertyUrl and (c.propertyUrl.uri in (uri, column))) or c.header == column):
                return c

        raise SchemaError(f'Dataset has no column "{column}" in table "{t.url}"')

    def _get_table(self, table: TableType) -> Table:
        if not isinstance(table, Table):
            uri = term_uri(table, terms=TERMS.by_uri)
            for t in self.tables:
                if (uri and t.common_props.get('dc:conformsTo') == uri) or t.url.string == table:
                    return t
            raise SchemaError(f'Dataset has no table "{table}"')
        if any(table is tt for tt in self.tables):
            return table
        raise SchemaError(f'Dataset has no table "{table}"')

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

    def get(self, item: SchemaObjectType, default=None) -> Union[csvw.Table, csvw.Column, None]:
        """
        Acts like `dict.get`.

        :param item: See  :meth:`~pycldf.dataset.Dataset.__getitem__`
        """
        try:
            return self[item]
        except SchemaError:
            return default

    def get_foreign_key_reference(
            self, table: TableType, column: ColType,
    ) -> Optional[tuple[csvw.Table, csvw.Column]]:
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
        return None

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

    # Editing dataset metadata or schema -----------------------------------------------------------
    def add_provenance(self, **kw: Any) -> None:
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
                k = f'prov:{k}'
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

    def add_table(self, url: str, *cols: ColSpecType, **kw: Any) -> csvw.Table:
        """
        Add a table description to the Dataset.

        :param url: The url property of the table.
        :param cols: Column specifications; anything accepted by :func:`pycldf.dataset.make_column`.
        :param kw: Recognized keywords:
            - `primaryKey`: specify the column(s) constituting the primary key of the table.
            - `description`: a description of the table.
        :return: The new table.
        """
        t = self.add_component({"url": url, "tableSchema": {"columns": []}}, *cols)
        if 'primaryKey' in kw:
            pk = kw.pop('primaryKey')
            if pk is not None and not isinstance(pk, list):
                pk = [pk]
            t.tableSchema.primaryKey = pk
        if kw.get('description'):
            t.common_props['dc:description'] = kw.pop('description')
        t.common_props.update(kw)
        return t

    def remove_table(self, table: TableType) -> None:
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

    def add_component(self, component: Union[str, dict], *cols: ColSpecType, **kw) -> csvw.Table:
        """
        Add a CLDF component to a dataset.

        .. seealso:: https://github.com/cldf/cldf/blob/master/README.md#cldf-components

        :param component: A component specified by name or as `dict` representing the JSON \
        description of the component.
        :param kw: Recognized keywords: \
            - `url`: a url property for the table;\
            - `description`: a description of the table.
        """
        component = make_table(component)

        if kw.get('url'):
            component.url = Link(kw['url'])
        if kw.get('description'):
            component.common_props['dc:description'] = kw['description']

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

        component._parent = self.tablegroup  # pylint: disable=W0212
        self.auto_constraints(component)
        return component

    def add_columns(self, table: TableType, *cols: ColSpecType) -> None:
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
                raise ValueError(f'Duplicate column name: {col.name}')
            if col.propertyUrl and col.propertyUrl.uri in existing:
                raise ValueError(f'Duplicate column property: {col.propertyUrl.uri}')
            table.tableSchema.columns.append(col)
        self.auto_constraints()

    def remove_columns(self, table: TableType, *cols: ColType) -> None:
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

    def rename_column(self, table: TableType, col: ColType, name: str) -> None:
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
            primary_c: Optional[ColType] = None,
    ) -> None:
        """
        Add a foreign key constraint.

        ..note:: Composite keys are not supported yet.

        :param foreign_t: Table reference for the linking table.
        :param foreign_c: Column reference for the link.
        :param primary_t: Table reference for the linked table.
        :param primary_c: Column reference for the linked column - or `None`, in which case the \
        primary key of the linked table is assumed.
        """
        return add_foreign_key(self, foreign_t, foreign_c, primary_t, primary_c)

    def auto_constraints(self, component: Optional[TableType] = None):
        """
        Use CLDF reference properties to implicitly create foreign key constraints.

        :param component: A Table object or `None`.
        """
        return add_auto_constraints(self, component)

    # Add data -------------------------------------------------------------------------------------
    def add_sources(self, *sources: Union[str, Source], **kw) -> None:
        """
        Add sources to the dataset.

        :param sources: Anything accepted by :meth:`pycldf.sources.Sources.add`.
        """
        self.sources.add(*sources, **kw)

    # Methods to read data -------------------------------------------------------------------------
    def iter_rows(self, table: TableType, *cols: str) -> Generator[RowType, None, None]:
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

    def cached_rows(self, table: TableType) -> list[RowType]:
        """Return the rows of a table from a cache."""
        key = table.local_name if isinstance(table, Table) else table
        if key not in self._cached_rows:
            self._cached_rows[key] = list(self.iter_rows(table))
        return self._cached_rows[key]

    def get_row(self, table: TableType, id_) -> RowType:
        """
        Retrieve a row specified by table and CLDF id.

        :raises ValueError: If no matching row is found.
        """
        id_col = self[table, TERMS['id']]
        for row in self[table]:
            if row[id_col.name] == id_:
                return row
        raise ValueError(id_)  # pragma: no cover

    def get_row_url(self, table: TableType, row: Union[RowType, str]) -> Optional[str]:
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
        row = self.get_row(table, row) if isinstance(row, str) else row
        id_col = None
        for col in self[table].tableSchema.columns:
            if col.datatype and col.datatype.base == datatypes.anyURI.__name__:
                # If one of the columns in the table explicitly spacifies anyURI as datatype, we
                # return the value of this column.
                return row[col.name]
            if str(col.propertyUrl) == 'http://cldf.clld.org/v1.0/terms.rdf#id':
                # Otherwise we fall back to looking up the `valueUrl` property on the ID column.
                id_col = col
        assert id_col, f'no ID column found in table {table}'
        if id_col.valueUrl:
            return id_col.valueUrl.expand(**row)
        return None

    def objects(self, table: str, cls: Optional[Type] = None) -> DictTuple:
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

    def get_object(self, table: str, id_: str, cls=None, pk=False) -> orm.Object:
        """
        Get a row of a component as :class:`pycldf.orm.Object` instance.
        """
        if table not in self._objects:
            self.objects(table, cls=cls)
        return self._objects[table][id_] if not pk else self._objects_by_pk[table][id_]

    # Methods for writing (meta)data to files: -----------------------------------------------------
    def write_metadata(self, fname: Optional[PathType] = None) -> pathlib.Path:
        """
        Write the CLDF metadata to a JSON file.

        :fname: Path of a file to write to, or `None` to use the default name and write to \
        :meth:`~pycldf.dataset.Dataset.directory`.
        """
        return self.tablegroup.to_file(fname or self.tablegroup._fname)  # pylint: disable=W0212

    def write_sources(self, zipped: bool = False) -> Optional[pathlib.Path]:
        """
        Write the sources BibTeX file to :meth:`~pycldf.dataset.Dataset.bibpath`

        :return: `None`, if no BibTeX file was written (because no source items were added), \
        `pathlib.Path` of the written BibTeX file otherwise. Note that this path does not need \
        to exist, because the content may have been added to a zip archive.
        """
        return self.sources.write(self.bibpath, zipped=zipped)

    def write(
            self,
            fname: Optional[pathlib.Path] = None,
            zipped: Optional[Iterable] = None,
            **table_items: list[RowType]
    ) -> pathlib.Path:
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

    def copy(self, dest: PathType, mdname: str = None) -> pathlib.Path:
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
        return copy_dataset(self, dest, mdname)

    # Reporting ------------------------------------------------------------------------------------
    def validate(
            self,
            log: logging.Logger = None,
            validators: list[tuple[Optional[str], str, validation.RowValidatorType]] = None,
            ontology_path: Optional[PathType] = None,
    ) -> bool:
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
        return validation.validate(
            dataset=self,
            terms=Terms(ontology_path) or TERMS,
            log=log,
            row_validators=validators or [],
        )

    def stats(self, exact: bool = False) -> list[tuple[str, str, int]]:
        """
        Compute summary statistics for the dataset.

        :return: List of triples (filename, component, rowcount).
        """
        return get_table_stats(self, exact)


class Generic(Dataset):
    """
    Generic datasets have no primary table.

    .. seealso:: `<https://github.com/cldf/cldf/tree/master/modules/Generic>`_
    """
    @property
    def primary_table(self) -> None:  # pylint: disable=missing-function-docstring
        return None


class Wordlist(Dataset):
    """
    Wordlists have support for segment slice notation.

    .. seealso:: `<https://github.com/cldf/cldf/tree/master/modules/Wordlist>`_
    """
    @property
    def primary_table(self) -> str:  # pylint: disable=missing-function-docstring
        return 'FormTable'

    def get_segments(self, row: RowType, table='FormTable') -> list[str]:
        """Retrieve the list of segments of a form."""
        col = self[table].get_column("http://cldf.clld.org/v1.0/terms.rdf#segments")
        sounds = row[col.name]
        if isinstance(sounds, str):
            # This may be the case when no morpheme boundaries are provided.
            sounds = [sounds]
        return list(itertools.chain(*[s.split() for s in sounds]))

    def get_subsequence(self, cognate: RowType, form: Optional[str] = None) -> list[str]:
        """
        Compute the subsequence of the morphemes of a form which is specified in a partial
        cognate assignment.

        :param cognate: A `dict` holding the data of a row from a `CognateTable`.
        """
        target_row = form or self.get_row('FormTable', cognate['Form_ID'])
        return multislice_with_split(
            target_row[self['FormTable', "http://cldf.clld.org/v1.0/terms.rdf#segments"].name],
            cognate[self['CognateTable', "http://cldf.clld.org/v1.0/terms.rdf#segmentSlice"].name],
        )


class ParallelText(Dataset):
    """Implements the CLDF ParallelText module."""
    @property
    def primary_table(self) -> str:  # pylint: disable=missing-function-docstring
        return 'FormTable'

    def get_equivalent(self, functional_equivalent, form=None):
        """Get the forms fulfilling an equivalent function in the texts."""
        slice_col_name = self[
            'FunctionalEquivalentTable', "http://cldf.clld.org/v1.0/terms.rdf#segmentSlice"].name
        sequence_col_name = self['FormTable', "http://cldf.clld.org/v1.0/terms.rdf#segments"].name
        target_row = form or self.get_row('FormTable', functional_equivalent['Form_ID'])
        return multislice_with_split(
            target_row[sequence_col_name], functional_equivalent[slice_col_name])


class Dictionary(Dataset):
    """Implements the CLDF Dictionary module."""
    @property
    def primary_table(self) -> str:  # pylint: disable=missing-function-docstring
        return 'EntryTable'


class StructureDataset(Dataset):
    """
    Parameters in StructureDataset are often called "features".

    .. seealso:: `<https://github.com/cldf/cldf/tree/master/modules/StructureDataset>`_
    """
    @property
    def primary_table(self) -> str:  # pylint: disable=missing-function-docstring
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
    def primary_table(self) -> str:  # pylint: disable=missing-function-docstring
        return 'ExampleTable'

    @functools.cached_property
    def texts(self) -> Optional[DictTuple]:
        """Retrieve texts."""
        # Some syntactic sugar to access the ORM data in a concise and meaningful way.
        if 'ContributionTable' in self:
            return self.objects('ContributionTable')
        return None  # pragma: no cover

    def get_text(self, tid: str) -> Optional[orm.Object]:
        """Retrieve a text by ID."""
        if 'ContributionTable' in self:
            return self.get_object('ContributionTable', tid)
        return None  # pragma: no cover

    @property
    def sentences(self) -> list[orm.Example]:
        """Sentences of the corpus."""
        res = list(self.objects('ExampleTable'))
        if ('ExampleTable', 'exampleReference') in self:
            # Filter out alternative translations!
            res = [e for e in res if not e.cldf.exampleReference]
        if ('ExampleTable', 'position') in self:
            return sorted(res, key=lambda o: o.cldf.position)
        return res  # pragma: no cover


def iter_datasets(d: PathType) -> Generator[Dataset, None, None]:
    """
    Discover CLDF datasets - by identifying metadata files - in a directory.

    :param d: directory in which to look for CLDF datasets (recursively).
    :return: generator of `Dataset` instances.
    """
    for p in walk(d, mode='files'):
        if sniff(p):
            try:
                yield Dataset.from_metadata(p)
            except ValueError as e:
                logging.getLogger(__name__).warning("Reading %s failed: %s", p, e)
