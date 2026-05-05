"""
Functionality to manage modules, i.e. `Dataset` subclasses implementing particular CLDF modules.
"""
import dataclasses
from typing import Union, Optional, Type

from csvw.metadata import TableGroup

from pycldf.terms import TERMS, term_uri
from pycldf.util import pkg_path, MD_SUFFIX

__all__ = ['get_module_impl']


@dataclasses.dataclass
class Module:
    """
    Class representing a CLDF Module.

    .. seealso:: https://github.com/cldf/cldf/blob/master/README.md#cldf-modules
    """
    uri: str
    fname: str

    def __post_init__(self):
        if self.uri not in {t.uri for t in TERMS.classes.values()}:
            raise ValueError(self.uri)  # pragma: no cover

    @property
    def id(self) -> str:
        """
        The local part of the term URI is interpreted as Module identifier.
        """
        return self.uri.split('#')[1]

    def match(self, thing: Union[TableGroup, str]) -> bool:
        """Check if the module described here matches thing."""
        if isinstance(thing, TableGroup):
            return thing.common_props.get('dc:conformsTo') == term_uri(self.id)
        if isinstance(thing, str):
            return thing == self.fname or thing == self.id
        return False


_modules = []


def get_module_impl(base_class, spec: Union[TableGroup, str]) -> Optional[Type]:
    """
    Returns an implementation (aka Dataset subclass) for a particular CLDF module.
    """
    implementations = {cls.__name__: cls for cls in base_class.__subclasses__()}
    for mod in get_modules():
        if mod.match(spec):
            return implementations[mod.id]
    return None  # pragma: no cover


def get_modules() -> list[Module]:
    """
    We read supported CLDF modules from the default metadata files distributed with `pycldf`.
    """
    global _modules  # pylint: disable=global-statement

    if not _modules:
        for p in pkg_path('modules').glob(f'*{MD_SUFFIX}'):
            tg = TableGroup.from_file(p)
            mod = Module(
                tg.common_props['dc:conformsTo'],
                tg.tables[0].url.string if tg.tables else None)
            _modules.append(mod)
        # prefer Wordlist over ParallelText (forms.csv)
        _modules = sorted(
            _modules,
            key=lambda m: (m.id in ('Wordlist', 'ParallelText'), m.id == 'ParallelText'))
    return _modules
