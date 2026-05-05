"""
This module provides a flexible implementation of slicing sequences, based on Python's slices.

In addition to Python's way of specifying slices as triples of integers (start, stop, step), we
allow specification as strings like '1' or '2:5', where the numbers are interpreted as **1-based**
indices, specifying **inclusive** boundaries. I.e. '2:5' is equivalent to `slice(1:5).`
"""
from typing import Union, TypeVar
import itertools
from collections.abc import Sequence, Iterable

__all__ = ['multislice', 'multislice_with_split']

T = TypeVar('T')
SliceType = Union[str, tuple[int], tuple[int, int], tuple[int, int, int], slice]


def multislice(sliceable: Sequence[T], *slices: SliceType) -> Sequence[T]:
    """
    .. code-block:: python

        >>> import string
        >>> multislice(list(range(30)), '3:7', '9', (12, 18, 3))
        [2, 3, 4, 5, 6, 8, 12, 15]
        >>> multislice(string.ascii_lowercase, '3:7', '9', (12, 18, 3))
        'cdefgimp'
    """
    res = type(sliceable)()
    for sl in slices:
        if isinstance(sl, str):
            if ':' in sl:
                assert sl.count(':') <= 2, f'String slice spec may only have two colons. {sl}'
                sl = slice(*[int(s) - (1 if i == 0 else 0) for i, s in enumerate(sl.split(':'))])
            else:
                sl = slice(*[int(sl) - 1, int(sl)])
        elif isinstance(sl, int):
            sl = slice(sl, sl + 1)
        elif isinstance(sl, (tuple, list)):
            sl = slice(*sl)
        else:
            assert isinstance(sl, slice)
        res += sliceable[sl]
    return res


def multislice_with_split(sliceable: Sequence[T], slices: Iterable[SliceType]) -> list[T]:
    """
    Resolves multislices and then applies splitting on whitespace to each item.

    .. code-block:: python

        >>> multislice_with_split(['a', 'b', 'c d', 'f', 'g'], [(2, 4)])
        ['c', 'd', 'f']
    """
    return list(itertools.chain(*[s.split() for s in multislice(sliceable, *slices)]))
