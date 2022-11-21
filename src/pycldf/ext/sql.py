"""
This module provides a function - :func:`get_database` - to create and use a CLDF SQL compliant
SQLite database.
"""
from pycldf import Database
from .discovery import get_dataset

__all__ = ['get_database']


def get_database(locator, download_dir=None, fname=None, base=None) -> Database:
    """
    :param locator: A resolvable dataset locator.
    :param download_dir: Optional path to a directory to download data for remote datasets.
    :param fname: Optional path of a non-existing file which will be used as SQLite database file.

    .. code-block:: python

        >>> import pathlib
        >>> from pycldf.ext.sql import get_database
        >>> dldir = pathlib.Path('/tmp/wacl')
        >>> dldir.mkdir()
        >>> db = get_database('https://doi.org/10.5281/zenodo.7322688', dldir)
        >>> query = '''SELECT
        ...     l.cldf_name, p.cldf_name, v.cldf_value
        ... FROM
        ...     LanguageTable AS l, ParameterTable AS p, ValueTable AS v
        ... WHERE
        ...     v.cldf_languageReference = l.cldf_id AND v.cldf_parameterReference = p.cldf_id'''
        >>> triples = db.query(query)
        >>> triples[0]
        ('Aari', 'Presence/absence of numeral classifiers', 'FALSE')
   """
    db = Database(get_dataset(locator, download_dir, base=base), fname=fname)
    db.write_from_tg()
    return db
