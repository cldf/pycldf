`pycldf.dataset`
================

The core object of the API, bundling most access to CLDF data, is
the `pycldf.Dataset`. In the following we'll describe its
attributes and methods, bundled into thematic groups.


Dataset initialization
~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: pycldf.dataset.Dataset
   :members: __init__, in_dir, from_metadata, from_data


Accessing dataset metadata
~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: pycldf.Dataset
   :noindex:
   :members: directory, module, version, metadata_dict, properties, bibpath, bibname


Accessing schema objects: components, tables, columns, etc.
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: pycldf.Dataset
   :noindex:
   :members: tables, components, __getitem__, __contains__, get, get_foreign_key_reference, column_names, readonly_column_names


Editing metadata and schema
~~~~~~~~~~~~~~~~~~~~~~~~~~~

In many cases, editing the metadata of a dataset is as simple as editing
:meth:`~pycldf.dataset.Dataset.properties`, but for the somewhat complex
formatting of provenance data, we provide the shortcut
:meth:`~pycldf.dataset.Dataset.add_provenance`.

Likewise, `csvw.Table` and `csvw.Column` objects in the dataset's schema can
be edited "in place", by setting their attributes or adding to/editing their
`common_props` dictionary.
Thus, the methods listed below are concerned with adding and removing tables
and columns.

.. autoclass:: pycldf.Dataset
   :noindex:
   :members: add_table, remove_table, add_component, add_columns, remove_columns, add_foreign_key, add_provenance,


Adding data
~~~~~~~~~~~

The main method to persist data as CLDF dataset is :meth:`~pycldf.Dataset.write`,
which accepts data for all CLDF data files as input. This does not include
sources, though. These must be added using :meth:`~pycldf.Dataset.add_sources`.

.. autoclass:: pycldf.Dataset
   :noindex:
   :members: add_sources


Reading data
~~~~~~~~~~~~

Reading rows from CLDF data files, honoring the datatypes specified in the schema,
is already implemented by `csvw`. Thus, the simplest way to read data is iterating
over the `csvw.Table` objects. However, this will ignore the semantic layer provided
by CLDF. E.g. a CLDF languageReference linking a value to a language will be appear
in the `dict` returned for a row under the local column name. Thus, we provide several
more convenient methods to read data.

.. autoclass:: pycldf.Dataset
   :noindex:
   :members: iter_rows, get_row, get_row_url, objects, get_object


Writing (meta)data
~~~~~~~~~~~~~~~~~~

.. autoclass:: pycldf.Dataset
   :noindex:
   :members: write, write_metadata, write_sources


Reporting
~~~~~~~~~

.. autoclass:: pycldf.Dataset
   :noindex:
   :members: validate, stats


Dataset discovery
~~~~~~~~~~~~~~~~~

We provide two functions to make it easier to discover CLDF datasets in the file system. This is useful, e.g., when downloading archived datasets from Zenodo, where it
may not be known in advance where in a zip archive the metadata file may reside.


.. autofunction:: pycldf.sniff

.. autofunction:: pycldf.iter_datasets


Sources
~~~~~~~

When constructing sources for a CLDF dataset in Python code, you may pass
:class:`pycldf.Source` instances into :meth:`pycldf.Dataset.add_sources`,
or use :meth:`pycldf.Reference.__str__` to format a row's `source` value
properly.

Direct access to :class:`pycldf.dataset.Sources` is rarely necessary (hence
it is not available as import from `pycldf` directly), because each
:class:`pycldf.Dataset` provides access to an apprpriately initialized instance
in its `sources` attribute.

.. autoclass:: pycldf.Source
   :members:

.. autoclass:: pycldf.Reference
   :members: __str__

.. autoclass:: pycldf.dataset.Sources
   :members:
