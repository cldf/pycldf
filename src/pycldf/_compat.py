"""
Backwards compatibility with supported python versions.
"""
import sys


if (sys.version_info.major, sys.version_info.minor) >= (3, 10):  # pragma: no cover
    def entry_points_select(eps, group):
        """
        Staring with Python 3.10, `importlib.metadata.entry_points` returns `EntryPoints`."""
        return eps.select(group=group)
else:
    def entry_points_select(eps, group):  # pragma: no cover
        """In Python 3.9, `importlib.metadata.entry_points` returns a `dict`."""
        return eps.get(group, [])
