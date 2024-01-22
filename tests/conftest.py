import io
import pathlib
import urllib.parse

import pytest
import csvw
import packaging.version

from pycldf import Dataset

DATA = pathlib.Path(__file__).parent / 'data'


@pytest.fixture(scope='module')
def data():
    return DATA


@pytest.fixture(scope='session')
def csvw3():
    return packaging.version.parse(csvw.__version__) > packaging.version.parse('2.0.0')


@pytest.fixture
def urlopen(mocker, data, csvw3):
    import requests_mock

    def _urlopen(url):
        return io.BytesIO(data.joinpath(urllib.parse.urlparse(url).path[1:]).read_bytes())

    mocker.patch('pycldf.sources.urlopen', _urlopen)
    if not csvw3:  # pragma: no cover
        mocker.patch('csvw.metadata.urlopen', _urlopen)
    else:
        mock = requests_mock.Mocker()
        mock.__enter__()
        mock.get(requests_mock.ANY, content=lambda req, _: _urlopen(req.url).read())


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
def textcorpus(data):
    return Dataset.from_metadata(data / 'textcorpus' / 'metadata.json')


@pytest.fixture(scope='module')
def structuredataset_with_examples(data):
    return Dataset.from_metadata(data / 'structuredataset_with_examples' / 'metadata.json')


@pytest.fixture
def dataset_with_media(data):
    dsdir = data / 'dataset_with_media'
    return Dataset.from_metadata(dsdir / 'metadata.json')


@pytest.fixture(scope='module')
def wordlist_with_borrowings(data):
    return Dataset.from_metadata(data / 'wordlist_with_borrowings' / 'metadata.json')


@pytest.fixture(scope='module')
def wordlist_with_cognates(data):
    return Dataset.from_metadata(data / 'wordlist_with_cognates' / 'metadata.json')


@pytest.fixture(scope='module')
def dataset_with_trees(data):
    return Dataset.from_metadata(data / 'dataset_with_trees' / 'metadata.json')


@pytest.fixture(scope='module')
def dataset_with_trees2(data):
    return Dataset.from_metadata(data / 'dataset_with_trees2' / 'metadata.json')
