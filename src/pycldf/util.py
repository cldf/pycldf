import pathlib
import itertools
import collections

import pycldf

__all__ = ['pkg_path', 'multislice', 'resolve_slices', 'DictTuple']


def pkg_path(*comps):
    return pathlib.Path(pycldf.__file__).resolve().parent.joinpath(*comps)


def multislice(sliceable, *slices):
    res = type(sliceable)()
    for sl in slices:
        if isinstance(sl, str):
            if ':' in sl:
                sl = [int(s) - (1 if i == 0 else 0) for i, s in enumerate(sl.split(':'))]
            else:
                sl = [int(sl) - 1, int(sl)]
        res += sliceable[slice(*sl)]
    return res


def resolve_slices(row, ds, slice_spec, target_spec, fk, target_row=None):
    # 1. Determine the slice column:
    slices = ds[slice_spec]

    # 2. Determine the to-be-sliced column:
    morphemes = ds[target_spec]

    # 3. Retrieve the matching row in the target table:
    target_row = target_row or ds.get_row(target_spec[0], row[fk])

    # 4. Slice the segments
    return list(itertools.chain(*[
        s.split() for s in multislice(target_row[morphemes.name], *row[slices.name])]))


class DictTuple(tuple):
    """
    A `tuple` that acts like a `dict` when a `str` is passed to `__getitem__`.

    Since CLDF requires a unique `id` for each row in a component, and recommends identifier of
    type `str`, this class can be used to provide convenient access to items in an ordered
    collection of such objects.
    """
    def __new__(cls, items, **kw):
        return super(DictTuple, cls).__new__(cls, tuple(items))

    def __init__(self, items, key=lambda i: i.id, multi=False):
        """
        If `key` does not return unique values for all items, you may pass `multi=True` to
        retrieve `list`s of matching items for `l[key]`.
        """
        self._d = collections.defaultdict(list)
        for i, o in enumerate(self):
            self._d[key(o)].append(i)
        self._multi = multi

    def __getitem__(self, item):
        if not isinstance(item, (int, slice)):
            if self._multi:
                return [self[i] for i in self._d[item]]
            return self[self._d[item][0]]
        return super(DictTuple, self).__getitem__(item)
