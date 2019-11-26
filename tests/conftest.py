import pathlib

import pytest

from pycldf import Dataset

DATA = pathlib.Path(__file__).parent / 'data'


@pytest.fixture(scope='module')
def data():
    return DATA


@pytest.fixture(scope='module')
def dataset(data):
    return Dataset.from_metadata(data / 'ds1.csv-metadata.json')
