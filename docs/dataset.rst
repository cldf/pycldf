`pycldf.dataset`
================

.. py:currentmodule:: pycldf.dataset

The core object of the API, bundling most access to CLDF data, is
the :class:`.Dataset` . In the following we'll describe its
attributes and methods, bundled into thematic groups.


Dataset initialization
~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: Dataset
   :members: __init__, in_dir, from_metadata, from_data


Accessing dataset metadata
~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autoproperty:: Dataset.directory
.. autoproperty:: Dataset.module
.. autoproperty:: Dataset.version
.. autoproperty:: Dataset.metadata_dict
.. autoproperty:: Dataset.properties
.. autoproperty:: Dataset.bibpath
.. autoproperty:: Dataset.bibname


Accessing schema objects: components, tables, columns, etc.
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Similar to *capability checks* in programming languages that use
`duck typing <https://en.wikipedia.org/wiki/Duck_typing>`_, it is often necessary
to access a datasets schema, i.e. its tables and columns, to figure out whether
the dataset fits a certain purpose. This is supported via a
`mapping <https://docs.python.org/3/glossary.html#term-mapping>`_-like interface provided
by :class:`.Dataset`, where the keys are table specifiers or pairs (table specifier, column specifier).
A *table specifier* can be a table's component name or its `url`, a *column specifier* can be a column
name or its `propertyUrl`.

* check existence with ``in``:

    .. code-block:: python

        if 'ValueTable' in dataset: ...
        if ('ValueTable', 'Language_ID') in dataset: ...

* retrieve a schema object with item access:

    .. code-block:: python

        table = dataset['ValueTable']
        column = dataset['ValueTable', 'Language_ID']

* retrieve a schema object or a default with :meth:`.Dataset.get`:

    .. code-block:: python

        table_or_none = dataset.get('ValueTableX')
        column_or_none = dataset.get(('ValueTable', 'Language_ID'))

* remove a schema object with ``del``:

    .. code-block:: python

        del dataset['ValueTable', 'Language_ID']
        del dataset['ValueTable']

.. note::

    Adding schema objects is **not** supported via key assignment, but with a set of specialized
    methods described in :ref:`Editing metadata and schema`.

.. autoproperty:: Dataset.tables
.. autoproperty:: Dataset.components
.. automethod:: Dataset.__getitem__
.. automethod:: Dataset.__delitem__
.. automethod:: Dataset.__contains__
.. automethod:: Dataset.get
.. automethod:: Dataset.get_foreign_key_reference
.. autoproperty:: Dataset.column_names
.. autoproperty:: Dataset.readonly_column_names


Editing metadata and schema
~~~~~~~~~~~~~~~~~~~~~~~~~~~

In many cases, editing the metadata of a dataset is as simple as editing
:meth:`.Dataset.properties`, but for the somewhat complex
formatting of provenance data, we provide the shortcut
:meth:`.Dataset.add_provenance`.

Likewise, ``csvw.Table`` and ``csvw.Column`` objects in the dataset's schema can
be edited "in place", by setting their attributes or adding to/editing their
``common_props`` dictionary.
Thus, the methods listed below are concerned with adding and removing tables
and columns.

.. automethod:: Dataset.add_table
.. automethod:: Dataset.remove_table
.. automethod:: Dataset.add_component
.. automethod:: Dataset.add_columns
.. automethod:: Dataset.remove_columns
.. automethod:: Dataset.rename_column
.. automethod:: Dataset.add_foreign_key
.. automethod:: Dataset.add_provenance


Adding data
~~~~~~~~~~~

The main method to persist data as CLDF dataset is :meth:`.Dataset.write`,
which accepts data for all CLDF data files as input. This does not include
sources, though. These must be added using :meth:`.Dataset.add_sources`.

.. automethod:: Dataset.add_sources



Reading data
~~~~~~~~~~~~

Reading rows from CLDF data files, honoring the datatypes specified in the schema,
is already implemented by `csvw`. Thus, the simplest way to read data is iterating
over the ``csvw.Table`` objects. However, this will ignore the semantic layer provided
by CLDF. E.g. a CLDF languageReference linking a value to a language will be appear
in the ``dict`` returned for a row under the local column name. Thus, we provide several
more convenient methods to read data.

.. automethod:: Dataset.iter_rows
.. automethod:: Dataset.get_row
.. automethod:: Dataset.get_row_url
.. automethod:: Dataset.objects
.. automethod:: Dataset.get_object


Writing (meta)data
~~~~~~~~~~~~~~~~~~

.. automethod:: Dataset.write
.. automethod:: Dataset.write_metadata
.. automethod:: Dataset.write_sources


Reporting
~~~~~~~~~

.. automethod:: Dataset.validate
.. automethod:: Dataset.stats


Dataset discovery
~~~~~~~~~~~~~~~~~

We provide two functions to make it easier to discover CLDF datasets in the file system. This is useful, e.g., when downloading archived datasets from Zenodo, where it
may not be known in advance where in a zip archive the metadata file may reside.


.. autofunction:: pycldf.sniff

.. autofunction:: pycldf.iter_datasets


Sources
~~~~~~~

When constructing sources for a CLDF dataset in Python code, you may pass
:class:`pycldf.Source` instances into :meth:`Dataset.add_sources`,
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


Subclasses supporting specific CLDF modules
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. note::

    Most functionality provided through properties and methods described below is implemented via
    the :mod:`pycldf.orm` module, and thus subject to the limitations listed at `<./orm.html>`_

.. autoclass:: pycldf.Generic
    :members:

.. autoclass:: pycldf.Wordlist
    :members:

.. autoclass:: pycldf.StructureDataset
    :members:

.. autoclass:: pycldf.TextCorpus
    :members:
