# pycldf

A python package to read and write [CLDF](http://cldf.clld.org) datasets.

[![Build Status](https://github.com/cldf/pycldf/workflows/tests/badge.svg)](https://github.com/cldf/pycldf/actions?query=workflow%3Atests)
[![Documentation Status](https://readthedocs.org/projects/pycldf/badge/?version=latest)](https://pycldf.readthedocs.io/en/latest/?badge=latest)
[![PyPI](https://img.shields.io/pypi/v/pycldf.svg)](https://pypi.org/project/pycldf)


## Install

Install `pycldf` from [PyPI](https://pypi.org/project/pycldf):
```shell
pip install pycldf
```


## Command line usage

Installing the `pycldf` package will also install a command line interface `cldf`, which provides some sub-commands to manage CLDF datasets.


### Dataset discovery

`cldf` subcommands support dataset discovery as specified in the [standard](https://github.com/cldf/cldf/blob/master/extensions/discovery.md).

So a typical workflow involving a remote dataset could look as follows.

Create a local directory to which to download the dataset (ideally including version info):
```shell
$ mkdir wacl-1.0.0
```

Validating a dataset from Zenodo will implicitly download it, so running
```shell
$ cldf validate https://zenodo.org/record/7322688#rdf:ID=wacl --download-dir wacl-1.0.0/
```
will download the dataset to `wacl-1.0.0`.

Subsequently we can access the data locally for better performance:
```shell
$ cldf stats wacl-1.0.0/#rdf:ID=wacl
<cldf:v1.0:StructureDataset at wacl-1.0.0/cldf>
                          value
------------------------  --------------------------------------------------------------------
dc:bibliographicCitation  Her, One-Soon, Harald Hammarström and Marc Allassonnière-Tang. 2022.
dc:conformsTo             http://cldf.clld.org/v1.0/terms.rdf#StructureDataset
dc:identifier             https://wacl.clld.org
dc:license                https://creativecommons.org/licenses/by/4.0/
dc:source                 sources.bib
dc:title                  World Atlas of Classifier Languages
dcat:accessURL            https://github.com/cldf-datasets/wacl
rdf:ID                    wacl
rdf:type                  http://www.w3.org/ns/dcat#Distribution

                Type              Rows
--------------  --------------  ------
values.csv      ValueTable        3338
parameters.csv  ParameterTable       1
languages.csv   LanguageTable     3338
codes.csv       CodeTable            2
sources.bib     Sources           2000
```

(Note that locating datasets on Zenodo requires installation of [cldfzenodo](https:pypi.org/project/cldfzenodo).)


### Summary statistics

```shell
$ cldf stats mydataset/Wordlist-metadata.json 
<cldf:v1.0:Wordlist at mydataset>

Path                   Type          Rows
---------------------  ----------  ------
forms.csv              Form Table       1
mydataset/sources.bib  Sources          1
```


### Validation

Arguably the most important functionality of `pycldf` is validating CLDF datasets.

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


### Extracting human readable metadata

The information in a CLDF metadata file can be converted to [markdown](https://en.wikipedia.org/wiki/Markdown)
(a human readable markup language) running
```shell
cldf markdown PATH/TO/metadata.json
```
A typical usage of this feature is to create a `README.md` for your dataset
(which, when uploaded to e.g. GitHub will be rendered nicely in the browser).


### Downloading media listed in a dataset's MediaTable

Typically, CLDF datasets only reference media items. The *MediaTable* provides enough information, though,
to download and save an item's content. This can be done running
```shell
cldf downloadmedia PATH/TO/metadata.json PATH/TO/DOWNLOAD/DIR
```
To minimize bandwidth usage, relevant items can be filtered by passing selection criteria in the form
`COLUMN_NAME=SUBSTRING` as optional arguments. E.g. downloading could be limited to audio files passing
`Media_Type=audio/` (provided, `Media_Type` is the name of the column with `propertyUrl` 
http://cldf.clld.org/v1.0/terms.rdf#mediaType)


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


## Python API

For a detailed documentation of the Python API, refer to the
[docs on ReadTheDocs](https://pycldf.readthedocs.io/en/latest/index.html).


### Reading CLDF

As an example, we'll read data from [WALS Online, v2020](https://github.com/cldf-datasets/wals/tree/v2020):

```python
>>> from pycldf import Dataset
>>> wals2020 = Dataset.from_metadata('https://raw.githubusercontent.com/cldf-datasets/wals/v2020/cldf/StructureDataset-metadata.json')
```

For exploratory purposes, accessing a remote dataset over HTTP is fine. But for real analysis, you'd want to download
the datasets first and then access them locally, passing a local file path to `Dataset.from_metadata`.

Let's look at what we got:
```python
>>> print(wals2020)
<cldf:v1.0:StructureDataset at https://raw.githubusercontent.com/cldf-datasets/wals/v2020/cldf/StructureDataset-metadata.json>
>>> for c in wals2020.components:
  ...     print(c)
...
ValueTable
ParameterTable
CodeTable
LanguageTable
ExampleTable
```
As expected, we got a [StructureDataset](https://github.com/cldf/cldf/tree/master/modules/StructureDataset), and in
addition to the required `ValueTable`, we also have a couple more [components](https://github.com/cldf/cldf#cldf-components).

We can investigate the values using [`pycldf`'s ORM](src/pycldf/orm.py) functionality, i.e. mapping rows in the CLDF
data files to convenient python objects. (Take note of the limitations describe in [orm.py](src/pycldf/orm.py), though.)

```python
>>> for value in wals2020.objects('ValueTable'):
  ...     break
...
>>> value
<pycldf.orm.Value id="81A-aab">
>>> value.language
<pycldf.orm.Language id="aab">
>>> value.language.cldf
Namespace(glottocode=None, id='aab', iso639P3code=None, latitude=Decimal('-3.45'), longitude=Decimal('142.95'), macroarea=None, name='Arapesh (Abu)')
>>> value.parameter
<pycldf.orm.Parameter id="81A">
>>> value.parameter.cldf
Namespace(description=None, id='81A', name='Order of Subject, Object and Verb')
>>> value.references
(<Reference Nekitel-1985[94]>,)
>>> value.references[0]
<Reference Nekitel-1985[94]>
>>> print(value.references[0].source.bibtex())
@misc{Nekitel-1985,
    olac_field = {syntax; general_linguistics; typology},
    school     = {Australian National University},
    title      = {Sociolinguistic Aspects of Abu', a Papuan Language of the Sepik Area, Papua New Guinea},
    wals_code  = {aab},
    year       = {1985},
    author     = {Nekitel, Otto I. M. S.}
}
```

If performance is important, you can just read rows of data as python `dict`s, in which case the references between
tables must be resolved "by hand":

```python
>>> params = {r['id']: r for r in wals2020.iter_rows('ParameterTable', 'id', 'name')}
>>> for v in wals2020.iter_rows('ValueTable', 'parameterReference'):
    ...     print(params[v['parameterReference']]['name'])
...     break
...
Order of Subject, Object and Verb
```

Note that we passed names of CLDF terms to `Dataset.iter_rows` (e.g. `id`) specifying which columns we want to access 
by CLDF term - rather than by the column names they are mapped to in the dataset.


## Writing CLDF

**Warning:** Writing CLDF with `pycldf` does not automatically result in valid CLDF!
It does result in data that can be checked via `cldf validate` (see [below](#validation)),
though, so you should always validate after writing.

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
- a full CLDF Ontolgy URI used as `propertyUrl` of the column,
- the `name` property of the column.

See also https://pycldf.readthedocs.io/en/latest/dataset.html#accessing-schema-objects-components-tables-columns-etc


## Object oriented access to CLDF data

The [`pycldf.orm`](src/pycldf/orm.py) module implements functionality
to access CLDF data via an [ORM](https://en.wikipedia.org/wiki/Object%E2%80%93relational_mapping).
See https://pycldf.readthedocs.io/en/latest/orm.html for
details.


## Accessing CLDF data via SQL

The [`pycldf.db`](src/pycldf/db.py) module implements functionality
to load CLDF data into a [SQLite](https://sqlite.org) database. See https://pycldf.readthedocs.io/en/latest/ext_sql.html
for details.


## See also
- https://github.com/frictionlessdata/datapackage-py
