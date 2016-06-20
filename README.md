pycldf
======

A python package to read and write [CLDF](http://cldf.clld.org) datasets

[![Build Status](https://travis-ci.org/glottobank/pycldf.svg?branch=master)](https://travis-ci.org/glottobank/pycldf)
[![codecov](https://codecov.io/gh/glottobank/pycldf/branch/master/graph/badge.svg)](https://codecov.io/gh/glottobank/pycldf)
[![Requirements Status](https://requires.io/github/glottobank/pycldf/requirements.svg?branch=master)](https://requires.io/github/glottobank/pycldf/requirements/?branch=master)
[![PyPI](https://img.shields.io/pypi/v/pycldf.svg)](https://pypi.python.org/pypi/pycldf)


Writing CLDF
------------

```python
from pycldf.dataset import Dataset
from pycldf.sources import Source
dataset = Dataset('mydb')
dataset.fields = ('ID', 'Language_ID', 'Parameter_ID', 'Value', 'Source', 'Comment')
dataset.sources.add(Source('book', 'Meier2005', author='Hans Meier', year='2005', title='The Book'))
dataset.add_row([
    '1', 
    'http://glottolog.org/resource/languoid/id/stan1295', 
    'http://concepticon.clld.org/parameters/1277', 
    'hand', 
    'Meier2005[3-7]', 
    ''])
dataset.write('.')
```

results in 

- `mydb.csv`
```
ID,Language_ID,Parameter_ID,Value,Source,Comment
1,http://glottolog.org/resource/languoid/id/stan1295,http://concepticon.clld.org/parameters/1277,hand,Meier2005[3-7],
```
- `mydb.bib`
```bibtex
@book{Meier2005,
    author = {Meier, Hans},
    title = {The Book},
    year = {2005}
}
```
- `mydb.csv-metadata.json`
```python
{
    "@context": [
        "http://www.w3.org/ns/csvw",
        {
            "@language": "en"
        }
    ],
    "dc:format": "cldf-1.0",
    "dialect": {
        "header": true,
        "delimiter": ",",
        "encoding": "utf-8"
    },
    "tables": [
        {
            "url": "",
            "dc:type": "cldf-values",
            "tableSchema": {
                "primaryKey": "ID",
                "columns": [
                    {
                        "datatype": "string",
                        "name": "ID"
                    },
                    {
                        "datatype": "string",
                        "name": "Language_ID"
                    },
                    {
                        "datatype": "string",
                        "name": "Parameter_ID"
                    },
                    {
                        "datatype": "string",
                        "name": "Value"
                    },
                    {
                        "datatype": "string",
                        "name": "Source"
                    },
                    {
                        "datatype": "string",
                        "name": "Comment"
                    }
                ]
            }
        }
    ]
}
```


Reading CLDF
------------

```python
>>> from pycldf.dataset import Dataset
>>> dataset = Dataset.from_file('mydb.csv')
>>> dataset
<Dataset mydb>
>>> len(dataset)
1
>>> row = dataset.rows[0]
>>> row
Row([('ID', u'1'), 
     ('Language_ID', 'http://glottolog.org/resource/languoid/id/stan1295'), 
     ('Parameter_ID', 'http://concepticon.clld.org/parameters/1277'), 
     ('Value', 'hand'), 
     ('Source', 'Meier2005[3-7]'), 
     ('Comment', '')])
>>> row['Value']
'hand'
>>> row.refs
[<Reference Meier2005[3-7]>]
>>> row.refs[0].source
<Source Meier2005>
>>> print row.refs[0].source
Meier, Hans. 2005. The Book.
>>> print row.refs[0].source.bibtex()
@book{Meier2005,
  year   = {2005},
  author = {Meier, Hans},
  title  = {The Book}
}
```


### Validating a data file

By default, data files are read in strict-mode, i.e. invalid rows will result in an exception
being raised. To validate a data file, it can be read in validating-mode.

For example the following output is generated

```python
>>> from pycldf.dataset import Dataset
>>> dataset = Dataset.from_file('mydb.csv', skip_on_error=True)
WARNING:pycldf.dataset:skipping row in line 3: wrong number of columns in row
WARNING:pycldf.dataset:skipping row in line 4: duplicate ID: 1
WARNING:pycldf.dataset:skipping row in line 5: missing citekey: Mei2005
```

when reading the file

```
ID,Language_ID,Parameter_ID,Value,Source,Comment
1,stan1295,1277,hand,Meier2005[3-7],
1,stan1295,1277,hand,Meier2005[3-7]
1,stan1295,1277,hand,Meier2005[3-7],
2,stan1295,1277,hand,Mei2005[3-7],
```


Support for augmented metadata
------------------------------

`pycldf` provides some support for metadata properties as described in 
[W3's Metadata Vocabulary for Tabular Data](https://www.w3.org/TR/tabular-metadata/), in particular,
- On [column description level](https://www.w3.org/TR/tabular-metadata/#dfn-column-description),
  - `datatype` is interpreted to use appropriate python objects internally,
  - a URI template provided as `valueUrl` can be expanded calling `Row.valueUrl(<colname>)`.
- On [schema description level](https://www.w3.org/TR/tabular-metadata/#dfn-schema-description),
  - a URI template provided as `aboutUrl` is used to compute the URL available as `Row.url`.

So the example above could be rewritten more succintly:

```python
from pycldf.dataset import Dataset
from pycldf.sources import Source
dataset = Dataset('mydb')
dataset.fields = ('ID', 'Language_ID', 'Parameter_ID', 'Value', 'Source', 'Comment')
dataset.table.schema.columns['ID'].datatype = int
dataset.table.schema.columns['Language_ID'].valueUrl = 'http://glottolog.org/resource/languoid/id/{Language_ID}'
dataset.table.schema.columns['Parameter_ID'].valueUrl = 'http://concepticon.clld.org/parameters/{Parameter_ID}'
dataset.sources.add(Source('book', 'Meier2005', author='Hans Meier', year='2005', title='The Book'))
dataset.add_row(['1', 'stan1295', '1277', 'hand', 'Meier2005[3-7]', ''])
dataset.write('.')
```

And then accessed as follows:

```python
>>> from pycldf.dataset import Dataset
>>> dataset = Dataset.from_file('mydb.csv')
>>> row = dataset.rows[0]
>>> type(row['ID'])
<type 'int'>
>>> row.valueUrl('Language_ID')
'http://glottolog.org/resource/languoid/id/stan1295'
>>> row['Language_ID']
'stan1295'
```
