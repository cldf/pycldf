import io
import pathlib
import urllib.parse

import pytest

from pycldf import Dataset

DATA = pathlib.Path(__file__).parent / 'data'


@pytest.fixture(scope='module')
def data():
    return DATA


@pytest.fixture
def urlopen(mocker, data):
    def _urlopen(url):
        return io.BytesIO(data.joinpath(urllib.parse.urlparse(url).path[1:]).read_bytes())

    mocker.patch('pycldf.sources.urlopen', _urlopen)
    mocker.patch('csvw.metadata.urlopen', _urlopen)


@pytest.fixture(scope='module')
def glottolog_repos():
    return DATA.parent / 'glottolog'


@pytest.fixture(scope='module')
def concepticon_repos():
    return DATA.parent / 'concepticon'


@pytest.fixture(scope='module')
def dataset(data):
    return Dataset.from_metadata(data / 'ds1.csv-metadata.json')


@pytest.fixture(scope='module')
def dictionary(data):
    return Dataset.from_metadata(data / 'dictionary' / 'metadata.json')


@pytest.fixture(scope='module')
def structuredataset_with_examples(data):
    return Dataset.from_metadata(data / 'structuredataset_with_examples' / 'metadata.json')


@pytest.fixture(scope='module')
def wordlist_with_borrowings(data):
    return Dataset.from_metadata(data / 'wordlist_with_borrowings' / 'metadata.json')


@pytest.fixture(scope='module')
def wordlist_with_cognates(data):
    return Dataset.from_metadata(data / 'wordlist_with_cognates' / 'metadata.json')
