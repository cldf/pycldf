# Changes

The `pycldf` package adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

## [1.38.0] - 2024-04-26

- Fixed bug whereby `dict` returned by `orm.Language.as_geojson_feature` could not be serialized
  by `json.dumps`.
- Fixed bug whereby SQLite conversion would fail when the name of a renamed column clashed with an
  existing column name in the table.
- Emit warning when encountering invalid main part for mediaType property.


## [1.37.1] - 2024-03-18

Fixed bug whereby component names where the CSV filenames contain underscores were not translated
appropriately when creating the SQLite db. (Note that this fix is required for the ParameterNetwork
component in CLDF 1.3.)


## [1.37.0] - 2024-01-22

Support for CLDF 1.3.


## [1.36.0] - 2023-11-14

- Support typed values, specified via `columnSpec` in `ParameterTable` in `orm` module.
- Better datatype descriptions when rendering metadata as markdown.
- Run tests on python 3.12


## [1.35.1] - 2023-10-23

- Fixed bug whereby checking existence of a URL was done too strictly.
- Fixed bug whereby non-file objects in the filesystem would trip up dataset detection.


## [1.35.0] - 2023-07-10

- Dropped py3.7 compatibility.
- Support zipped data files properly when creating metadata descriptions in Markdown.


## [1.34.1] - 2023-03-15

- Switch from `python-nexus` to `commonnexus` for reading NEXUS files.


## [1.34.0] - 2022-12-05

- Support reading and writing sources from/to zipped BibTeX files.
- Load `Dataset.sources` lazily. (See https://github.com/cldf/pycldf/issues/162)


## [1.33.0] - 2022-11-24

- Allow access to the unparsed Newick string for a tree.
- Cache parsed tree files in `TreeTable` for efficient validation.
- Validate existince of local files referenced in MediaTable.
- Validate readability of file and data URLs referenced in MediaTable.


## [1.32.0] - 2022-11-23

Better support for CLDF Markdown.
- Support somewhat efficient data access in CLDFMarkdownText.
- Fix bug whereby CLDF Markdown links to rows in custom tables were erased by
  FilenameToComponent.
- Fix bug whereby CLDF Markdown links to metadata were not recognized as such.


## [1.31.0] - 2022-11-22

Support data locators as input for all `cldf` subcommands.


## [1.30.0] - 2022-11-22

Fully supports CLDF 1.2 now, including [CLDF extensions](https://github.com/cldf/cldf#extensions).


## [1.29.0] - 2022-10-28

- Added support to write datasets with (individually) zipped table CSV files.


## [1.28.0] - 2022-10-11

- Added `pycldf.Dataset.filename` property, to retrieve the name of the metadat file for both cases, local and remote datasets.
- Enhanced cli utilities to make dataset specification and retrieval by URL possible.


## [1.27.0] - 2022-07-07

- Updates to account for CLDF 1.1.3
- Support for renaming columns
- Validation of URITemplate properties when validating


## [1.26.1] - 2022-05-23

- Fixed bug whereby zipped tables were not detected/evaluated during validation.


## [1.26.0] - 2022-05-19

- Dropped python 3.6 support
- Support for media download


## [1.25.1] - 2022-02-06

- Fixed bug whereby some last name parts of authors would not be included
  in `pycldf.source.Source.refkey`.


## [1.25.0] - 2022-02-05

- Enhanced ORM to make it more usable for e.g. templating.


## [1.24.0] - 2021-11-24

- Add python 3.10 to supported versions
- Don't leak git credentials from remote URLs into CLDF metadata
- Fleshed out and documented access to schema objects in a CLDF dataset


##  [1.23.0] - 2021-08-15

- Support copying datasets (see #143)
- Added `Source.refkey` method (see #142)


##  [1.22.0] - 2021-06-04

- Fixed bug whereby CSV files with large field content could not be read due to
  Python's `csv.field_size_limit`. Would have been a patch release, but requires
  `clldutils>=3.9` now, making this a minor change.


## [1.21.2] - 2021-05-28

- Fixed regression (see #140)


## [1.21.1] - 2021-05-26

- Updates to account for CLDF 1.1.2


## [1.21.0] - 2021-05-10

- Support for "typed values" in `StructureDataset`s
- More convenient access to targets of foreign key constraints via
  `Dataset.get_foreign_key_reference`
- Existence of components/columns can now be checked with `name in Dataset`  
- Added `Dataset.components` property, to allow convenient access to components
  specified in a dataset.
- Enhanced and refactored documentation (see https://pycldf.readthedocs.io/en/latest/). 

