import pytest

from pycldf import Dataset

from pycldf.ext.discovery import *


def test_get_dataset_local(data, tmp_path):
    assert get_dataset('structuredataset_with_examples', tmp_path, base=data)


def test_get_dataset_url(structuredataset_with_examples, tmp_path, mocker):
    class DummyDataset(Dataset):
        @classmethod
        def from_metadata(cls, fname):
            return cls(structuredataset_with_examples.tablegroup)

    mocker.patch('pycldf.ext.discovery.Dataset', DummyDataset)
    assert get_dataset('http://example.org', tmp_path)

    with pytest.raises(ValueError):
        get_dataset('http://example.org#rdf:ID=1', tmp_path)
