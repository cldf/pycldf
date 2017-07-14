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
from pycldf.dataset import Wordlist
from pycldf.sources import Source
dataset = Wordlist.in_dir('mydataset')
dataset.sources.add(Source('book', 'Meier2005', author='Hans Meier', year='2005', title='The Book'))
dataset.write(FormTable=[
    {
        'ID': '1', 
        'Value': 'word', 
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
```python
{
    "@context": "http://www.w3.org/ns/csvw", 
    "dc:conformsTo": "http://cldf.clld.org/v1.0/terms.rdf#Wordlist", 
    "dc:source": "sources.bib", 
    "dialect": {
        "commentPrefix": null
    }, 
    "tables": [
        {
            "dc:conformsTo": "http://cldf.clld.org/v1.0/terms.rdf#FormTable", 
            "tableSchema": {
                "columns": [
                    {
                        "datatype": "string", 
                        "propertyUrl": "http://purl.org/dc/terms/identifier", 
                        "required": true, 
                        "name": "ID"
                    }, 
                    {
                        "datatype": "string", 
                        "propertyUrl": "http://linguistics-ontology.org/gold/2010/Language", 
                        "required": true, 
                        "name": "Language_ID"
                    }, 
                    {
                        "datatype": "string", 
                        "propertyUrl": "http://www.w3.org/2004/02/skos/core#Concept", 
                        "required": true, 
                        "name": "Parameter_ID", 
                        "titles": "Concept_ID"
                    }, 
                    {
                        "datatype": "string", 
                        "propertyUrl": "http://linguistics-ontology.org/gold/2010/FormUnit", 
                        "required": true, 
                        "name": "Value"
                    }, 
                    {
                        "datatype": "string", 
                        "propertyUrl": "http://linguistics-ontology.org/gold/2010/Segment", 
                        "separator": " ", 
                        "name": "Segments"
                    }, 
                    {
                        "datatype": "string", 
                        "propertyUrl": "http://purl.org/dc/terms/description", 
                        "name": "Comment"
                    }, 
                    {
                        "datatype": "string", 
                        "propertyUrl": "http://purl.org/dc/terms/source", 
                        "separator": ";", 
                        "name": "Source"
                    }
                ], 
                "primaryKey": [
                    "ID"
                ]
            }, 
            "url": "forms.csv"
        }
    ]
}
```


Reading CLDF
------------

```python
>>> from pycldf.dataset import Wordlist
>>> dataset = Wordlist.from_metadata('mydataset/Wordlist-metadata.json')
>>> print(dataset)
<cldf:v1.0:Wordlist at mydataset>
>>> forms = list(dataset['FormTable'])
>>> forms[0]
OrderedDict([('ID', '1'), ('Language_ID', 'abcd1234'), ('Parameter_ID', '1277'), ('Value', 'word'), ('Segments', []), ('Comment', None), ('Source', ['Meier2005[3-7]'])])
>>> refs = list(dataset.sources.expand_refs(forms[0]['Source']))
>>> refs
[<Reference Meier2005[3-7]>]
>>> print(refs[0].source)
Meier, Hans. 2005. The Book.
```


Command line usage
------------------

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
