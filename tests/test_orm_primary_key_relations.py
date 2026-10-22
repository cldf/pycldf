import pytest

from pycldf import StructureDataset, Wordlist


@pytest.mark.parametrize('colliding_ids', [False, True])
def test_plural_scalar_relation_uses_primary_key(tmp_path, colliding_ids):
    ds = StructureDataset.in_dir(tmp_path)
    ds.add_component('ParameterTable')
    ids = ['b', 'a'] if colliding_ids else ['1', '2']
    ds['ParameterTable'].tableSchema.primaryKey = ['Name']
    ds['ValueTable'].tableSchema.foreignKeys[0].reference.columnReference = ['Name']
    ds.write(
        ParameterTable=[dict(ID=ids[0], Name='a'), dict(ID=ids[1], Name='b')],
        ValueTable=[dict(ID='v', Language_ID='l', Parameter_ID='a', Value='x')])
    assert ds.validate()
    value = ds.get_object('ValueTable', 'v')
    assert value.parameter.id == ids[0]
    assert [p.id for p in value.parameters] == [ids[0]]


def test_list_relation_preserves_primary_key_order_and_duplicates(tmp_path):
    ds = Wordlist.in_dir(tmp_path)
    ds.add_component('ParameterTable')
    ds['ParameterTable'].tableSchema.primaryKey = ['Name']
    ds['FormTable', 'Parameter_ID'].separator = ';'
    ds['FormTable'].tableSchema.foreignKeys[0].reference.columnReference = ['Name']
    ds.write(
        ParameterTable=[dict(ID='a', Name='b'), dict(ID='b', Name='a')],
        FormTable=[dict(ID='f', Language_ID='l', Parameter_ID=['b', 'a', 'b'], Form='x')])
    assert ds.validate()
    form = ds.get_object('FormTable', 'f')
    assert [p.id for p in form.parameters] == ['a', 'b', 'a']


def test_plural_relation_rejects_unsupported_foreign_key_target(tmp_path):
    ds = StructureDataset.in_dir(tmp_path)
    ds.add_component('ParameterTable')
    ds['ValueTable'].tableSchema.foreignKeys[0].reference.columnReference = ['Name']
    ds.write(
        ParameterTable=[dict(ID='a', Name='b'), dict(ID='b', Name='a')],
        ValueTable=[dict(ID='v', Language_ID='l', Parameter_ID='a', Value='x')])
    value = ds.get_object('ValueTable', 'v')
    with pytest.raises(NotImplementedError):
        _ = value.parameters
