# coding: utf8
from __future__ import unicode_literals, print_function, division
import itertools

from clldutils.path import Path
from six import string_types

import pycldf


def pkg_path(*comps):
    return Path(pycldf.__file__).resolve().parent.joinpath(*comps)


def multislice(sliceable, *slices):
    res = type(sliceable)()
    for sl in slices:
        if isinstance(sl, string_types):
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
