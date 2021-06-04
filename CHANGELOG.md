# Changes

The `pycldf` package adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

## [Unreleased]


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

