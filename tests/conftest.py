from __future__ import unicode_literals

import pytest

from clldutils import path

DATA = path.Path(__file__).parent / 'data'


@pytest.fixture(scope='module')
def data():
    return DATA
