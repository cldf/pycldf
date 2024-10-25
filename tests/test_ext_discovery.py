import shutil
import urllib.parse

import pytest

from pycldf import Dataset

from pycldf.ext.discovery import *


def test_get_dataset_local(data, tmp_path):
    assert get_dataset('structuredataset_with_examples', tmp_path, base=data)


def test_get_dataset_github(data, tmp_path, mocker):
    def urlretrieve(url, p):
        url = urllib.parse.urlparse(url)
        assert url.netloc == 'github.com'
        assert url.path.startswith('/cldf-datasets/petersonsouthasia')
        shutil.copy(data / 'petersonsouthasia-1.1.zip', p)

    mocker.patch('pycldf.ext.discovery.urllib.request.urlretrieve', urlretrieve)
    ds = get_dataset('https://github.com/cldf-datasets/petersonsouthasia/v1.1', tmp_path)
    assert (ds.properties["dc:title"] ==
            "Towards a linguistic prehistory of eastern-central South Asia")


def test_get_dataset_url(structuredataset_with_examples, tmp_path, mocker):
    class DummyDataset(Dataset):
        @classmethod
        def from_metadata(cls, fname):
            return cls(structuredataset_with_examples.tablegroup)

    mocker.patch('pycldf.ext.discovery.Dataset', DummyDataset)
    assert get_dataset('http://example.org', tmp_path)

    with pytest.raises(ValueError):
        get_dataset('http://example.org#rdf:ID=1', tmp_path)
