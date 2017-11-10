# coding: utf8
from __future__ import unicode_literals, print_function, division

from clldutils.path import Path
from six import string_types

import pycldf


def pkg_path(*comps):
    return Path(pycldf.__file__).resolve().parent.joinpath(*comps)


def multislice(sliceable, *slices):
    res = type(sliceable)()
    for sl in slices:
        if isinstance(sl, string_types):
            sl = map(int, sl.split(':'))
        res += sliceable[slice(*sl)]
    return res
