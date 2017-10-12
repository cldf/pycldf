# Examples


## Creating CLDF StructureDatasets from WALS

Prerequisites:
 
- A postgresql 9.x database loaded with the [WALS 2014 SQL dump from `clld/wals-data`](https://github.com/clld/wals-data/blob/master/wals2014-07.sql.gz).

  The schema of this database is the [clld](https://github.com/clld/clld) core
schema, augmented with the models defined for the [WALS app](https://github.com/clld/wals3).

- Python 2.7 or 3.4+ with `sqlalchemy`, `psycopg2`, `pycldf` and `clldutils`
  installed.
  
Now we can run the script [`wals2cldf.py`](wals2cldf.py) as follows:
```bash
(pycldf)dlt5502178l:~/venvs/pycldf/pycldf/examples$ python wals2cldf.py "postgresql://robert@/wals3" 1A
```
and inspect the directory it created:
```bash
(pycldf)dlt5502178l:~/venvs/pycldf/pycldf/examples$ ls -ks1 wals_1A_cldf/
total 196
 28 languages.csv
  4 parameters.csv
124 sources.bib
  8 StructureDataset-metadata.json
 32 values.csv
```
For further inspection we can use the `cldf` command:
```bash
(pycldf)dlt5502178l:~/venvs/pycldf/pycldf/examples$ cldf validate wals_1A_cldf/StructureDataset-metadata.json 
(pycldf)dlt5502178l:~/venvs/pycldf/pycldf/examples$ cldf stats wals_1A_cldf/StructureDataset-metadata.json
<cldf:v1.0:StructureDataset at wals_1A_cldf>
key            value
-------------  ----------------------------------------------------
dc:source      sources.bib
dc:conformsTo  http://cldf.clld.org/v1.0/terms.rdf#StructureDataset

Path            Type               Rows
--------------  ---------------  ------
values.csv      Value Table         563
languages.csv   Language Table      563
parameters.csv  Parameter Table       1
codes.csv       Code Table            5
sources.bib     Sources             947
```