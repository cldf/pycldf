# Changes

The `pycldf` package adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

## [Unreleased]


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

