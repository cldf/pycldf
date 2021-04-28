# Examples

## Reading and writing CLDF data

As an extended example for reading and writing CLDF data with `pycldf`, we will
extract a single WALS feature as "stand-alone" CLDF dataset from the full WALS
Online v2020 data at https://doi.org/10.5281/zenodo.3731125 .

The same data is also available [from GitHub](https://github.com/cldf-datasets/wals/tree/v2020)
in a form that `pycldf` can access directly, i.e. without first downloading and unzipping
the packed version of the dataset.

Now we can run the script [`wals2cldf.py`](wals2cldf.py) as follows:
```
$ python wals2cldf.py 1A
```
Please inspect the heavily documented, short script [`wals2cldf.py`](wals2cldf.py) for idiomatic use of
`pycldf` functionalties.

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
