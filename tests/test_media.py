import logging
import zipfile
import urllib.parse

import pytest

from pycldf import Generic
from csvw.metadata import URITemplate
from pycldf.media import read_data_url, read_file_url, read_http_url, Mimetype, MediaTable, File


@pytest.fixture
def ds_factory(tmp_path):
    def factory(media_item):
        ds_ = Generic.in_dir(tmp_path)
        ds_.add_component('MediaTable', 'url')
        ds_['MediaTable', 'Media_Type'].required = False
        ds_.write(MediaTable=[media_item])
        return Generic.from_metadata(tmp_path / 'Generic-metadata.json')
    return factory


@pytest.fixture
def file_factory(ds_factory):
    def factory(media_item):
        ds = ds_factory(media_item)
        return list(MediaTable(ds))[0]
    return factory


def test_File(file_factory, mocker, tmp_path):
    mocker.patch(
        'pycldf.media.urllib.request',
        mocker.Mock(
            Request=lambda *args, **kw: mocker.Mock(),
            urlopen=lambda *args: mocker.Mock(
                headers={'Content-Type': 'application/json'},
                read=lambda: 'äöü'.encode('utf8'))))

    file = file_factory(dict(
        ID='123',
        Download_URL='data:,abc',
        Media_Type='text/plain;charset=ISO-8859-1'))
    assert file.mimetype.encoding == 'ISO-8859-1'
    assert file.mimetype.is_text
    file.save(tmp_path)
    assert tmp_path.joinpath('123.txt').exists()
    assert file.read(tmp_path) == 'abc'
    assert file['ID'] == '123'

    file = file_factory(dict(
        ID='123',
        Download_URL='http://example.org/test.png',
        Media_Type=''))
    assert file.mimetype.type == 'image'

    file = file_factory(dict(
        ID='123',
        Download_URL='data:image/png;base64,123',
        Media_Type=''))
    assert file.mimetype.type == 'image'

    file = file_factory(dict(
        ID='123',
        Download_URL='data:,123',
        Media_Type=''))
    assert file.mimetype.is_text

    file = file_factory(dict(
        ID='123',
        Download_URL='http://example.org/stuff',
        Media_Type=''))
    assert file.mimetype.type == 'application'

    file = file_factory(dict(
        ID='123',
        Download_URL='file:/stuff',
        Media_Type=''))
    assert file.local_path(tmp_path).name == '123.bin'
    assert file.mimetype.subtype == 'octet-stream'

    file = file_factory(dict(
        ID='123',
        Download_URL='file:/stuff',
        Media_Type='custom/stuff'))
    assert file.local_path(tmp_path).name == '123'


@pytest.mark.parametrize(
    'url,data',
    [
        ('data:,Hello%2C%20World%21', 'Hello, World!'),
        ('data:text/plain;base64,SGVsbG8sIFdvcmxkIQ==', 'Hello, World!'),
        ('data:text/html,%3Ch1%3EHello%2C%20World%21%3C%2Fh1%3E', '<h1>Hello, World!</h1>'),
        ("data:text/html,<script>alert('hi');</script>", "<script>alert('hi');</script>"),
        ('data:image/png;base64,SGVsbG8sIFdvcmxkIQ==', 'Hello, World!'),
    ]
)
def test_read_data_url(url, data):
    assert read_data_url(
        urllib.parse.urlparse(url), Mimetype('text/plain;charset=US-ASCII')) == data


def test_read_file_url(tmp_path):
    p = tmp_path / 'test.txt'
    p.write_text('äöü', encoding='utf16')
    res = read_file_url(
        tmp_path,
        urllib.parse.urlparse('file:/{}'.format(p.name)),
        Mimetype('text/plain;charset=UTF-16'))
    assert res == 'äöü'

    p.write_bytes('äöü'.encode('utf8'))
    res = read_file_url(
        tmp_path,
        urllib.parse.urlparse('file:/{}'.format(p.name)),
        Mimetype('image/jpg'))
    assert res == 'äöü'.encode('utf8')


def test_read_http_url(mocker):
    mocker.patch(
        'pycldf.media.urllib.request',
        mocker.Mock(urlopen=lambda *args: mocker.Mock(read=lambda: 'äöü'.encode('utf8'))))
    assert read_http_url(urllib.parse.urlparse('u'), Mimetype('text/plain;charset=UTF-8')) == 'äöü'
    assert read_http_url(urllib.parse.urlparse('u'), Mimetype('image/jpg')) == 'äöü'.encode('utf8')


def test_Media_invalid(ds_factory):
    ds = ds_factory(dict(
        ID='123',
        Download_URL='data:text/plain;base64,abc',
        Media_Type='text/plain;charset=ISO-8859-1'))
    with pytest.raises(ValueError):
        ds.validate()


def test_Media(tmp_path, ds_factory):
    ds = ds_factory(dict(
        ID='123',
        Download_URL='file:/test.txt',
        Media_Type='text/plain;charset=ISO-8859-1'))
    ds['MediaTable', 'ID'].valueUrl = URITemplate('data:,{ID}')
    assert File.from_dataset(
        ds,
        dict(ID='x', Media_Type='image/png', Download_URL='')).mimetype.type == 'image'
    assert File.from_dataset(
        ds,
        ds.get_object('MediaTable', '123')).mimetype.type == 'text'
    with pytest.raises(ValueError) as e:
        ds.validate()
    media = MediaTable(ds)
    tmp_path.joinpath('test.txt').write_bytes('äöü'.encode('latin1'))
    assert ds.validate()
    assert 'äöü' == list(media)[0].read()

    ds['MediaTable', 'Download_URL'].propertyUrl = ''  # Now the valueUrl kicks in!
    media = MediaTable(ds)
    assert '123' == list(media)[0].read()

    ds['MediaTable', 'ID'].valueUrl = URITemplate('')
    media = MediaTable(ds)
    assert list(media)[0].read() is None

    ds = ds_factory(dict(
        ID='123',
        url='file:/test.txt',
        Media_Type='text/plain;charset=ISO-8859-1'))
    ds['MediaTable', 'Download_URL'].propertyUrl = ''
    ds['MediaTable', 'url'].propertyUrl = URITemplate('http://www.w3.org/ns/dcat#downloadUrl')
    media = MediaTable(ds)
    assert 'äöü' == list(media)[0].read()

    ds = ds_factory(dict(
        ID='123',
        Download_URL='filex:/test.txt',
        Media_Type='text/plain;charset=ISO-8859-1'))
    media = MediaTable(ds)
    with pytest.raises(ValueError):
        list(media)[0].read()

    with zipfile.ZipFile(str(tmp_path / '123.zip'), 'w') as zf:
        zf.writestr('arc/name', 'äöü'.encode('utf8'))
    ds = ds_factory(dict(
        ID='123',
        Download_URL="file:///123.zip",
        Media_Type='text/plain',
        Path_In_Zip='arc/name',
    ))
    assert list(MediaTable(ds))[0].read() == 'äöü'
    assert list(MediaTable(ds))[0].read(d=tmp_path) == 'äöü'


def test_save_read_zipped_media(dataset_with_trees, tmp_path):
    zipped = None
    for f in MediaTable(dataset_with_trees):
        f.save(tmp_path)
        if f.path_in_zip:
            zipped = f
    zfs = list(tmp_path.glob('*.zip'))
    assert len(zfs) == 1
    with zipfile.ZipFile(zfs[0]) as zf:
        assert 'nexus.trees' in zf.namelist()
    assert zipped.read(tmp_path).startswith('#NEXUS')


def test_Media_validate(tmp_path):
    ds = Generic.in_dir(tmp_path)
    ds.add_component('MediaTable')
    ds.remove_columns('MediaTable', 'Download_URL')
    ds['MediaTable', 'ID'].valueUrl = ''
    ds.write(MediaTable=[dict(ID='123', Media_Type='text/plain')])
    assert not ds.validate(log=logging.getLogger('test'))


def test_Media_validate2(dataset_with_media):
    assert dataset_with_media.validate()
