from typing import Union

from csvw.metadata import Column, Table
from clldutils import jsonlib

from pycldf.terms import TERMS
from pycldf.util import MD_SUFFIX, pkg_path

ColSpecType = Union[str, dict, Column]
TableSpecType = Union[str, dict, Table]
TableType = Union[str, Table]
ColType = Union[str, Column]


def make_column(spec: ColSpecType) -> Column:
    """
    Create a `Column` instance from `spec`.

    .. code-block:: python

        >>> make_column('id').name
        'id'
        >>> make_column('http://cldf.clld.org/v1.0/terms.rdf#id').name
        'ID'
        >>> make_column({'name': 'col', 'datatype': 'boolean'}).datatype.base
        'boolean'
        >>> type(make_column(make_column('id')))
        <class 'csvw.metadata.Column'>
    """
    if isinstance(spec, str):
        if spec in TERMS.by_uri:
            return TERMS.by_uri[spec].to_column()
        return Column(name=spec, datatype='string')
    if isinstance(spec, dict):
        return Column.fromvalue(spec)
    if isinstance(spec, Column):
        return spec
    raise TypeError(spec)


def make_table(spec: TableSpecType) -> Table:
    if isinstance(spec, str):
        return Table.fromvalue(jsonlib.load(pkg_path('components', f'{spec}{MD_SUFFIX}')))
    if isinstance(spec, dict):
        return Table.fromvalue(spec)
    if isinstance(spec, Table):
        return spec
    raise TypeError(spec)  # pragma: no cover
