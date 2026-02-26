"""
Functionality to compute summary statistics for a Dataset.
"""
import typing
import dataclasses
from collections.abc import Generator

from pycldf.terms import TERMS

if typing.TYPE_CHECKING:
    from pycldf import Dataset  # pragma: no cover

__all__ = ['get_table_stats']


def get_table_stats(ds: 'Dataset', exact: bool = False) -> list[tuple[str, str, int]]:
    """Return a list of table statistics."""
    return [dataclasses.astuple(stats) for stats in _iter_stats(ds, exact)]


@dataclasses.dataclass(frozen=True)
class TableStats:
    """A bag of attrs"""
    fname: str
    component: str
    rowcount: int


def _iter_stats(ds: 'Dataset', exact: bool = False) -> Generator[TableStats, None, None]:
    for table in ds.tables:
        dctype = table.common_props.get('dc:conformsTo')
        if dctype and '#' in dctype and dctype.split('#')[1] in TERMS:
            dctype = TERMS[dctype.split('#')[1]].csvw_prop('name')
        yield TableStats(
            table.url.string,
            dctype or '',
            sum(1 for _ in table) if (exact or 'dc:extent' not in table.common_props)
            else int(table.common_props.get('dc:extent')))
    if ds.sources:
        yield TableStats(ds.bibname, 'Sources', len(ds.sources))
