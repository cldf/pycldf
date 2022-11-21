from pycldf.ext.sql import *


def test_get_database(structuredataset_with_examples, tmp_path):
    res = get_database(
        str(structuredataset_with_examples.directory / 'metadata.json'),
        tmp_path,
    )
    assert res.query('select count(*) from exampletable')[0][0] > 1
