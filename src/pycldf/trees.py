"""
Support for the CLDF TreeTable component.

The peculiarity of a tree object in CLDF lies in the fact that the actual tree data is pulled in
from a media file in newick or Nexus format. This "pulling in" is implemented in the method
:meth:`Tree.newick`.

Accessing `Tree` instances associated with a dataset is done using a :class:`Trees` instance.

.. code-block:: python

    >>> from pycldf import Dataset
    >>> from pycldf.trees import TreeTable
    >>> ds = Dataset.from_metadata('tests/data/dataset_with_trees/metadata.json')
    >>> trees = list(TreeTable(ds))
    >>> print(trees[0].newick().ascii_art())
              ┌─l1
         ┌────┤
         │    └─l2
    ─────┤
         ├─l3
         └─l4
"""
import typing
import logging
import pathlib

from clldutils.misc import log_or_raise
from commonnexus import Nexus
import newick

import pycldf
from pycldf.media import MediaTable, File


__all__ = ['Tree', 'TreeTable']


class Tree:
    """
    Represents a tree object as specified in a row of `TreeTable`.
    """
    def __init__(self, trees: 'TreeTable', row: dict, file: File):
        self.row = row
        self.id = row[trees.cols['id'].name]
        self.name = row[trees.cols['name'].name]
        self.file = file
        for prop in ['description', 'treeType', 'treeIsRooted', 'treeBranchLengthUnit']:
            attrib = ''.join('_' + c.lower() if c.isupper() else c for c in prop)
            setattr(self, attrib, row.get(trees.cols[prop].name) if trees.cols[prop] else None)
        self.trees = trees

    def newick_string(self, d: typing.Optional[pathlib.Path] = None) -> str:
        """
        Retrieve the Newick representation of the tree from the associated tree file.

        :param d: Directory where the tree file was saved earlier, using \
        :meth:`pycldf.media.File.save`.
        :return: Newick representation of the associated tree.
        """
        if self.file.id not in self.trees._parsed_files:
            content = self.file.read(d=d)
            if self.file.mimetype == 'text/x-nh':
                self.trees._parsed_files[self.file.id] = {
                    str(index): nwk for index, nwk in enumerate(
                        [t.strip() for t in content.split(';') if t.strip()], start=1)}
            else:
                self.trees._parsed_files[self.file.id] = {
                    tree.name: tree.newick_string for tree in Nexus(content).TREES.trees}

        return self.trees._parsed_files[self.file.id][self.name]

    def newick(self,
               d: typing.Optional[pathlib.Path] = None,
               strip_comments: bool = False) -> newick.Node:
        """
        Retrieve a `newick.Node` instance for the tree from the associated tree file.

        :param d: Directory where the tree file was saved earlier, using \
        :meth:`pycldf.media.File.save`.
        :param strip_comments: Flag signaling whether to strip comments enclosed in square \
        brackets.
        :return: `newick.Node` representing the root of the associated tree.
        """
        return newick.loads(self.newick_string(d=d), strip_comments=strip_comments)[0]


class TreeTable(pycldf.ComponentWithValidation):
    """
    Container class for a `Dataset`'s TreeTable.
    """
    def __init__(self, ds: pycldf.Dataset):
        super().__init__(ds)
        self.media = MediaTable(ds)
        self.media_rows = {row[self.media.id_col.name]: row for row in ds['MediaTable']}
        self.cols = {
            prop: self.ds.get((self.table, prop)) for prop in [
                'id', 'name', 'description', 'mediaReference',
                'treeIsRooted', 'treeType', 'treeBranchLengthUnit']}
        # Since reading and parsing tree files is expensive, we cache them.
        self._parsed_files = {}

    def __iter__(self) -> typing.Generator[Tree, None, None]:
        for row in self.table:
            yield Tree(
                self,
                row,
                File(self.media, self.media_rows[row[self.cols['mediaReference'].name]]))

    def validate(self,
                 success: bool = True,
                 log: logging.Logger = None) -> bool:
        lids = {r['id'] for r in self.ds.iter_rows('LanguageTable', 'id')}
        for tree in self:
            try:
                nwk = tree.newick()
            except KeyError:
                log_or_raise(
                    'No newick tree found for name "{}"'.format(tree.name),
                    log=log)
                success = False
                nwk = None

            if nwk:
                for node in nwk.walk():
                    if node.name and (node.name not in lids):
                        log_or_raise(
                            'Newick node label "{}" is not a LanguageTable ID'.format(node.name),
                            log=log)
                        success = False
        return success
