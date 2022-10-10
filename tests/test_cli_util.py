import argparse

import pytest

from pycldf.cli_util import *


def test_UrlOrPathType(mocker):
    mocker.patch(
        'pycldf.cli_util.requests',
        mocker.Mock(head=mocker.Mock(return_value=mocker.Mock(status_code=200))))
    url = 'http://example.com'
    assert UrlOrPathType()(url) == url

    mocker.patch(
        'pycldf.cli_util.requests',
        mocker.Mock(head=mocker.Mock(return_value=mocker.Mock(status_code=404))))
    with pytest.raises(argparse.ArgumentTypeError):
        _ = UrlOrPathType()(url)
