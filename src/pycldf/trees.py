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
from typing import TYPE_CHECKING, Optional
import pathlib
from collections.abc import Generator

from commonnexus import Nexus
import newick
from csvw.metadata import Table, Column

from pycldf.media import MediaTable, File

if TYPE_CHECKING:
    from pycldf import Dataset  # pragma: no cover
    from pycldf.dataset import RowType  # pragma: no cover
    from pycldf.validators import DatasetValidator  # pragma: no cover

__all__ = ['Tree', 'TreeTable']


class Tree:
    """
    Represents a tree object as specified in a row of `TreeTable`.
    """
    def __init__(self, trees: 'TreeTable', row: 'RowType', file: File):
        self.row: 'RowType' = row
        self.id: str = row[trees.cols['id'].name]
        self.name: str = row[trees.cols['name'].name]
        self.file: File = file
        for prop in ['description', 'treeType', 'treeIsRooted', 'treeBranchLengthUnit']:
            attrib = ''.join('_' + c.lower() if c.isupper() else c for c in prop)
            setattr(self, attrib, row.get(trees.cols[prop].name) if trees.cols[prop] else None)
        self.trees = trees

    def newick_string(self, d: Optional[pathlib.Path] = None) -> str:
        """
        Retrieve the Newick representation of the tree from the associated tree file.

        :param d: Directory where the tree file was saved earlier, using \
        :meth:`pycldf.media.File.save`.
        :return: Newick representation of the associated tree.
        """
        if self.file.id not in self.trees.parsed_files:
            content = self.file.read(d=d)
            if self.file.mimetype == 'text/x-nh':
                self.trees.parsed_files[self.file.id] = {  # pylint: disable=protected-access
                    str(index): nwk for index, nwk in enumerate(
                        [t.strip() for t in content.split(';') if t.strip()], start=1)}
            else:
                self.trees.parsed_files[self.file.id] = {  # pylint: disable=protected-access
                    tree.name: tree.newick_string for tree in Nexus(content).TREES.trees}

        return self.trees.parsed_files[self.file.id][self.name]  # pylint: disable=protected-access

    def newick(self, d: Optional[pathlib.Path] = None, strip_comments: bool = False) -> newick.Node:
        """
        Retrieve a `newick.Node` instance for the tree from the associated tree file.

        :param d: Directory where the tree file was saved earlier, using \
        :meth:`pycldf.media.File.save`.
        :param strip_comments: Flag signaling whether to strip comments enclosed in square \
        brackets.
        :return: `newick.Node` representing the root of the associated tree.
        """
        return newick.loads(self.newick_string(d=d), strip_comments=strip_comments)[0]


class TreeTable:
    """
    Container class for a `Dataset`'s TreeTable.
    """
    def __init__(self, ds: 'Dataset'):
        self.ds: 'Dataset' = ds
        self.component: str = self.__class__.__name__
        self.table: Table = ds[self.component]
        self.media: MediaTable = MediaTable(ds)
        self.media_rows: dict[str, 'RowType'] = {
            row[self.media.id_col.name]: row for row in ds['MediaTable']}
        self.cols: dict[str, Optional[Column]] = {
            prop: self.ds.get((self.table, prop)) for prop in [
                'id', 'name', 'description', 'mediaReference',
                'treeIsRooted', 'treeType', 'treeBranchLengthUnit']}
        # Since reading and parsing tree files is expensive, we cache them.
        self.parsed_files: dict[str, dict[str, str]] = {}

    def __iter__(self) -> Generator[Tree, None, None]:
        for row in self.table:
            yield Tree(
                self,
                row,
                File(self.media, self.media_rows[row[self.cols['mediaReference'].name]]))

    def validate(self, validator: 'DatasetValidator'):
        """
        Makes sure Newick representations of trees are available and only reference valid languages.
        """
        lids = {r['id'] for r in self.ds.iter_rows('LanguageTable', 'id')}
        for tree in self:
            try:
                nwk = tree.newick()
            except KeyError:
                validator.fail(f'No newick tree found for name "{tree.name}"')
                nwk = None

            if nwk:
                for node in nwk.walk():
                    if node.name and (node.name not in lids):
                        validator.fail(f'Newick node label "{node.name}" is not a LanguageTable ID')
