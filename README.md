# pycldf

A python package to read and write [CLDF](http://cldf.clld.org) datasets.

[![Build Status](https://github.com/cldf/pycldf/workflows/tests/badge.svg)](https://github.com/cldf/pycldf/actions?query=workflow%3Atests)
[![codecov](https://codecov.io/gh/cldf/pycldf/branch/master/graph/badge.svg)](https://codecov.io/gh/cldf/pycldf)
[![Requirements Status](https://requires.io/github/cldf/pycldf/requirements.svg?branch=master)](https://requires.io/github/cldf/pycldf/requirements/?branch=master)
[![PyPI](https://img.shields.io/pypi/v/pycldf.svg)](https://pypi.org/project/pycldf)


## Reading CLDF

```python
>>> from pycldf.dataset import Dataset
>>> dataset = Dataset.from_metadata('mydataset/Wordlist-metadata.json')
>>> print(dataset)
<cldf:v1.0:Wordlist at mydataset>

# what is the type of dataset?
>>> print(dataset.module)
'Wordlist'

# iterate over forms:
>>> for form in dataset['FormTable']:
>>>    print(form)
>>> [('ID', '1'), ('Language_ID', 'abcd1234'), ('Parameter_ID', '1277'), ('Value', 'word'), ('Segments', []), ('Comment', None), ('Source', ['Meier2005[3-7]'])]
...

# or get all of them
>>> forms = list(dataset['FormTable'])
>>> forms[0]
OrderedDict([('ID', '1'), ('Language_ID', 'abcd1234'), ('Parameter_ID', '1277'), ('Value', 'word'), ('Segments', []), ('Comment', None), ('Source', ['Meier2005[3-7]'])])

# references
>>> refs = list(dataset.sources.expand_refs(forms[0]['Source']))
>>> refs
[<Reference Meier2005[3-7]>]
>>> print(refs[0].source)
Meier, Hans. 2005. The Book.
```


## Writing CLDF

```python
from pycldf import Wordlist, Source

dataset = Wordlist.in_dir('mydataset')
dataset.add_sources(Source('book', 'Meier2005', author='Hans Meier', year='2005', title='The Book'))
dataset.write(FormTable=[
    {
        'ID': '1', 
        'Form': 'word', 
        'Language_ID': 'abcd1234', 
        'Parameter_ID': '1277', 
        'Source': ['Meier2005[3-7]'],
    }])
```

results in
```
$ ls -1 mydataset/
forms.csv
sources.bib
Wordlist-metadata.json
```

- `mydataset/forms.csv`
```
ID,Language_ID,Parameter_ID,Value,Segments,Comment,Source
1,abcd1234,1277,word,,,Meier2005[3-7]
```
- `mydataset/sources.bib`
```bibtex
@book{Meier2005,
    author = {Meier, Hans},
    year = {2005},
    title = {The Book}
}

```
- `mydataset/Wordlist-metadata.json`


### Advanced writing

To add predefined CLDF components to a dataset, use the `add_component` method:
```python
from pycldf import StructureDataset, term_uri

dataset = StructureDataset.in_dir('mydataset')
dataset.add_component('ParameterTable')
dataset.write(
    ValueTable=[{'ID': '1', 'Language_ID': 'abc', 'Parameter_ID': '1', 'Value': 'x'}],
	ParameterTable=[{'ID': '1', 'Name': 'Grammatical Feature'}])
```

It is also possible to add generic tables:
```python
dataset.add_table('contributors.csv', term_uri('id'), term_uri('name'))
```
which can also be linked to other tables:
```python
dataset.add_columns('ParameterTable', 'Contributor_ID')
dataset.add_foreign_key('ParameterTable', 'Contributor_ID', 'contributors.csv', 'ID')
```

### Addressing tables and columns

Tables in a dataset can be referenced using a `Dataset`'s `__getitem__` method,
passing
- a full CLDF Ontology URI for the corresponding component,
- the local name of the component in the CLDF Ontology,
- the `url` of the table.

Columns in a dataset can be referenced using a `Dataset`'s `__getitem__` method,
passing a tuple `(<TABLE>, <COLUMN>)` where `<TABLE>` specifies a table as explained
above and `<COLUMN>` is
- a full CLD Ontolgy URI used as `propertyUrl` of the column,
- the `name` property of the column.


## Object oriented access to CLDF data

The [`pycldf.orm`](src/pycldf/orm.py) module implements functionality
to access CLDF data via an [ORM](https://en.wikipedia.org/wiki/Object%E2%80%93relational_mapping). Read its docstring for
details.


## Accessing CLDF data via SQL

The [`pycldf.db`](src/pycldf/db.py) module implements functionality
to load CLDF data into a [SQLite](https://sqlite.org) database. Read its docstring for
details.


## Command line usage

Installing the `pycldf` package will also install a command line interface `cldf`, which provides some sub-commands to manage CLDF datasets.


### Summary statistics

```sh
$ cldf stats mydataset/Wordlist-metadata.json 
<cldf:v1.0:Wordlist at mydataset>

Path                   Type          Rows
---------------------  ----------  ------
forms.csv              Form Table       1
mydataset/sources.bib  Sources          1
```


### Validation

By default, data files are read in strict-mode, i.e. invalid rows will result in an exception
being raised. To validate a data file, it can be read in validating-mode.

For example the following output is generated

```sh
$ cldf validate mydataset/forms.csv
WARNING forms.csv: duplicate primary key: (u'1',)
WARNING forms.csv:4:Source missing source key: Mei2005
```

when reading the file

```
ID,Language_ID,Parameter_ID,Value,Segments,Comment,Source
1,abcd1234,1277,word,,,Meier2005[3-7]
1,stan1295,1277,hand,,,Meier2005[3-7]
2,stan1295,1277,hand,,,Mei2005[3-7]
```


### Converting a CLDF dataset to an SQLite database

A very useful feature of CSVW in general and CLDF in particular is that it
provides enough metadata for a set of CSV files to load them into a relational
database - including relations between tables. This can be done running the
`cldf createdb` command:

```shell script
$ cldf createdb -h
usage: cldf createdb [-h] [--infer-primary-keys] DATASET SQLITE_DB_PATH

Load a CLDF dataset into a SQLite DB

positional arguments:
  DATASET               Dataset specification (i.e. path to a CLDF metadata
                        file or to the data file)
  SQLITE_DB_PATH        Path to the SQLite db file
```

For a specification of the resulting database schema refer to the documentation in
[`src/pycldf/db.py`](src/pycldf/db.py).


## See also
- https://github.com/frictionlessdata/datapackage-py
