# coding: utf8
from __future__ import unicode_literals, print_function, division

from clldutils.path import Path

import pycldf


def pkg_path(*comps):
    return Path(pycldf.__file__).parent.joinpath(*comps)
