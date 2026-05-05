"""
Functionality to access and manipulate files.
"""
import re
import math
import string
from typing import Union, Optional
import pathlib
import itertools


PathType = Union[str, pathlib.Path]


def splitfile(p: PathType, chunksize: int, total: Optional[int] = None) -> list[pathlib.Path]:
    """
    :param p: Path of the file to split.
    :param chunksize: The maximal size of the chunks the file will be split into.
    :param total: The size of the input file.
    :return: The list of paths of files that the input has been split into.
    """
    p = pathlib.Path(p)
    total = total or p.stat().st_size
    if total <= chunksize:  # Nothing to do.
        return [p]
    nchunks = math.ceil(total / chunksize)
    suffix_length = 2 if nchunks < len(string.ascii_lowercase)**2 else 3
    suffixes = [
        ''.join(t) for t in
        itertools.combinations_with_replacement(string.ascii_lowercase, suffix_length)]

    res = []
    with p.open('rb') as f:
        chunk = f.read(chunksize)
        while chunk:
            pp = p.parent.joinpath(f'{p.name}.{suffixes.pop(0)}')
            pp.write_bytes(chunk)
            res.append(pp)
            chunk = f.read(chunksize)  # read the next chunk

    p.unlink()
    return res


def catfile(p: PathType) -> bool:
    """
    Restore a file that has been split into chunks.

    We determine if a file has been split by looking for files in the parent directory with suffixes
    as created by `splitfile`.
    """
    p = pathlib.Path(p)
    if p.exists():  # Nothing to do.
        return False
    # Check, whether the file has been split.
    suffixes = {pp.suffix: pp for pp in p.parent.iterdir() if pp.stem == p.name}
    if {'.aa', '.ab'}.issubset(suffixes) or {'.aaa', '.aab'}.issubset(suffixes):
        # ok, let's concatenate the files:
        with p.open('wb') as f:
            for suffix in sorted(suffixes):
                if re.fullmatch(r'\.[a-z]{2,3}', suffix):
                    f.write(suffixes[suffix].read_bytes())
                    suffixes[suffix].unlink()
        return True
    return False  # pragma: no cover
