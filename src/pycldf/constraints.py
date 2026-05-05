"""
Functionality for creation of foreign key constraints.
"""
from typing import TYPE_CHECKING, Optional

from pycldf.terms import TERMS, term_uri
from pycldf.schemautil import TableType, ColType

if TYPE_CHECKING:
    from pycldf.dataset import Dataset  # pragma: no cover

__all__ = ['add_foreign_key', 'add_auto_constraints']


def add_foreign_key(
        ds: 'Dataset',
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
    if isinstance(foreign_c, (tuple, list)) or isinstance(primary_c, (tuple, list)):
        raise NotImplementedError('composite keys are not supported')

    foreign_t = ds[foreign_t]
    primary_t = ds[primary_t]
    if not primary_c:
        primary_c = primary_t.tableSchema.primaryKey
    else:
        primary_c = ds[primary_t, primary_c].name
    foreign_t.add_foreign_key(ds[foreign_t, foreign_c].name, primary_t.url.string, primary_c)


def add_auto_constraints(ds: 'Dataset', component: Optional[TableType] = None):
    """
    Use CLDF reference properties to implicitly create foreign key constraints.

    :param component: A Table object or `None`.
    """
    if not component:
        for table in ds.tables:
            ds.auto_constraints(table)
        return

    if not component.tableSchema.primaryKey:
        idcol = component.get_column(term_uri('id'))
        if idcol:
            component.tableSchema.primaryKey = [idcol.name]

    _auto_foreign_keys(ds, component)

    try:
        table_type = ds.get_tabletype(component)
    except ValueError:
        table_type = None

    if table_type is None:
        # New component is not a known CLDF term, so cannot add components
        # automatically. TODO: We might me able to infer some based on
        # `xxxReference` column properties?
        return

    # auto-add foreign keys targeting the new component:
    for table in ds.tables:
        _auto_foreign_keys(ds, table, component=component, table_type=table_type)


def _auto_foreign_keys(ds: 'Dataset', table, component=None, table_type=None):
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
                    ref = ds[ref_name]
                except KeyError:
                    continue
            else:
                ref = component
            idcol = ref.get_column(term_uri('id'))
            table.add_foreign_key(
                col.name, ref.url.string, idcol.name if idcol is not None else 'ID')
