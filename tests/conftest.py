import pathlib

import pytest

DATA = pathlib.Path(__file__).parent / 'data'


@pytest.fixture(scope='module')
def data():
    return DATA
