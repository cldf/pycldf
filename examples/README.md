# Examples


## Creating CLDF StructureDatasets from WALS

Prerequisites:
 
- A postgresql 9.x database loaded with a [WALS SQL dump](http://cdstar.shh.mpg.de//bitstreams/EAEA0-D1D1-3398-141A-0/wals_sql_dump.gz).
```
$ createdb walstutorial
$ gunzip -c wals_sql_dump.gz | psql walstutorial
```
The schema of this database is the [clld](https://github.com/clld/clld) core
schema, augmented with the models defined for the
[WALS app](https://github.com/clld/wals3).

- Python 2.7 or 3.4+ with `sqlalchemy`, `psycopg2`, `pycldf` and `clldutils`
  installed.
  
Now we can run the script [`wals2cldf.py`](wals2cldf.py) as follows
(substitute `POSTGRESUSER` with a user who has read-access to your local
database):
```
$ python wals2cldf.py "postgresql://POSTGRESUSER@/walstutorial" 1A
```
This packages the values of [feature 1A](http://wals.info/feature/1A) as CLDF
StructureDataset and we can now inspect the directory it created:
```
$ ls -ks1 wals_1A_cldf/
total 212
 12 StructureDataset-metadata.json
  4 codes.csv
 40 languages.csv
  4 parameters.csv
124 sources.bib
 28 values.csv
```
For further inspection we can use the `cldf` command:
```
$ cldf validate wals_1A_cldf/StructureDataset-metadata.json
$ cldf stats wals_1A_cldf/StructureDataset-metadata.json
<cldf:v1.0:StructureDataset at wals_1A_cldf>
key            value
-------------  ----------------------------------------------------
dc:conformsTo  http://cldf.clld.org/v1.0/terms.rdf#StructureDataset
dc:source      sources.bib

Path            Type              Rows
--------------  --------------  ------
values.csv      ValueTable         563
languages.csv   LanguageTable      563
parameters.csv  ParameterTable       1
codes.csv       CodeTable            5
sources.bib     Sources            947
```

## Creating CLDF Wordlist from WOLD

Prerequisites:
 
- A postgresql 9.x database loaded with a [WOLD SQL dump](http://cdstar.shh.mpg.de//bitstreams/EAEA0-D1D1-3398-141A-0/wold2_sql_dump.gz).
```
$ createdb woldtutorial
$ gunzip -c wold2_sql_dump.gz | psql woldtutorial
```
The schema of this database is the [clld](https://github.com/clld/clld) core
schema, augmented with the models defined for the [WOLD app](https://github.com/clld/wold2).

- Python 2.7 or 3.4+ with `sqlalchemy`, `psycopg2`, `pycldf` and `clldutils`
  installed.
  
Now we can run the script [`wold2cldf.py`](wals2cldf.py) as follows
(substitute `POSTGRESUSER` with a user who has read-access to your local
database):
```
$ python wals2cldf.py "postgresql://POSTGRESUSER@/woldtutorial" 1
```
to package the [Swahili vocabulary](http://wold.clld.org/vocabulary/1) as CLDF Wordlist,
and inspect the directory it created:
```
$ ls -ks1 wold_1_cldf/
 12 Wordlist-metadata.json
 32 borrowings.csv
 72 forms.csv
  4 languages.csv
 60 parameters.csv
```
