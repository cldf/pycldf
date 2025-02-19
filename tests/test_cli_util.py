import argparse

import pytest

from pycldf.cli_util import *


def test_UrlOrPathType(mocker):
    mocker.patch('pycldf.cli_util.http_head_status', mocker.Mock(return_value=200))
    url = 'http://example.com'
    assert UrlOrPathType()(url) == url

    mocker.patch('pycldf.cli_util.http_head_status', mocker.Mock(return_value=404))
    with pytest.raises(argparse.ArgumentTypeError):
        _ = UrlOrPathType()(url)
