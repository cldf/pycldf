pycldf
======

A python package to read and write [CLDF](http://cldf.clld.org) datasets

[![Build Status](https://travis-ci.org/glottobank/pycldf.svg?branch=master)](https://travis-ci.org/glottobank/pycldf)
[![codecov](https://codecov.io/gh/glottobank/pycldf/branch/master/graph/badge.svg)](https://codecov.io/gh/glottobank/pycldf)
[![Requirements Status](https://requires.io/github/glottobank/pycldf/requirements.svg?branch=master)](https://requires.io/github/glottobank/pycldf/requirements/?branch=master)
[![PyPI](https://img.shields.io/pypi/v/pycldf.svg)](https://pypi.python.org/pypi/pycldf)


Reading CLDF
------------

```python
>>> from pycldf.dataset import Dataset
>>> dataset = Dataset.from_file('mydb.csv')
>>> len(dataset)
1
>>> dataset.rows[0]
OrderedDict([(u'ID', u'1'), (u'Language_ID', u'http://glottolog.org/resource/languoid/id/stan1295'), (u'Parameter_ID', u'http://concepticon.clld.org/parameters/1277'), (u'Value', u'hand'), (u'Source', u'Meier2005[3-7]'), (u'Comment', u'')])
```


Writing CLDF
------------

```python
>>> from pycldf.dataset import Dataset
>>> from pycldf.sources import Source
>>> dataset = Dataset('mydb')
>>> dataset.fields = ('ID', 'Language_ID', 'Parameter_ID', 'Value', 'Source', 'Comment')
>>> dataset.sources.add(Source('Meier2005', 'book', author='Hans Meier', year='2005', title='The Book'))
>>> dataset.add_row([
... '1', 
... 'http://glottolog.org/resource/languoid/id/stan1295', 
... 'http://concepticon.clld.org/parameters/1277', 
... 'hand', 
... 'Meier2005[3-7]', 
... ''])
>>> dataset.write('.')
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
    "dialect": {
        "header": true,
        "delimiter": ",",
        "encoding": "utf-8"
    },
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
    },
    "dc:format": "cldf-1.0"
}
```
