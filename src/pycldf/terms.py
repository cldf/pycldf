"""
Functionality to access the metadata about CLDF schema objects encoded in the ontology.
"""
import re
import json
import types
import pathlib
import warnings
import dataclasses
import urllib.parse
from typing import Optional, Union, Callable, Any, TYPE_CHECKING
from collections.abc import Container
from xml.etree import ElementTree

import attr
from csvw.metadata import Column
from clldutils import jsonlib

from pycldf.util import pkg_path
from pycldf.fileutil import PathType

if TYPE_CHECKING:
    from pycldf import Dataset  # pragma: no cover

__all__ = ['term_uri', 'TERMS', 'get_column_names', 'sniff']

URL = 'http://cldf.clld.org/v1.0/terms.rdf'
RDF = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#'
RDFS = 'http://www.w3.org/2000/01/rdf-schema#'
CSVW = 'http://www.w3.org/ns/csvw#'
DC = 'http://purl.org/dc/terms/'


def qname(ns: str, lname: str) -> str:
    """Return a qualified name in ElementTree notation."""
    return '{%s}%s' % (ns, lname)


@dataclasses.dataclass
class NameSpec:  # pylint: disable=C0115
    ns: str
    lname: str

    @property
    def qname(self):  # pylint: disable=C0116
        return qname(self.ns, self.lname)


def _get(
        e: ElementTree.Element,
        subelement: NameSpec,
        attrib: Optional[NameSpec] = None,
        converter: Optional[Callable[[str], Any]] = None,
):
    """
    :return: Text content or attribute value of a subelement of e.
    """
    res = None
    subelement = e.find(subelement.qname)
    if subelement is not None:
        if not attrib:
            res = subelement.text
        else:
            res = subelement.attrib[attrib.qname]
    if converter and res:
        res = converter(res)
    return res


@attr.s
class Term:
    """A Term is an object described in the CLDF Ontology."""
    name: str = attr.ib()
    type: str = attr.ib(validator=attr.validators.in_(['Class', 'Property']))
    element: ElementTree.Element = attr.ib()
    references = attr.ib(default=None)
    subtype = attr.ib(default=None)
    version = attr.ib(default=None, validator=attr.validators.matches_re(r'v[0-9]+(\.[0-9]+)+'))
    cardinality = attr.ib(
        default=None,
        validator=attr.validators.optional(attr.validators.in_(['singlevalued', 'multivalued'])))

    @property
    def uri(self) -> str:
        """The Term URI."""
        return f'{URL}#{self.name}'

    @classmethod
    def from_element(cls, e: ElementTree.Element) -> 'Term':
        """Instantiate a Term from an XML element parsed from the ontology."""
        subClassOf = e.find(qname(RDFS, 'subClassOf'))  # pylint: disable=invalid-name
        kw = {
            'name': e.attrib[qname(RDF, 'about')].split('#')[1],
            'version': _get(
                e,
                NameSpec(ns=DC, lname='hasVersion'),
                attrib=NameSpec(ns=RDF, lname='resource'),
                converter=lambda s: 'v' + s.split('/v')[1].replace('/', '')) or 'v1.0',
            'type': e.tag.split('}')[1],
            'element': e,
            'cardinality': _get(e, NameSpec(ns=DC, lname='extent')),
            'references': _get(
                e,
                NameSpec(ns=DC, lname='references'),
                attrib=NameSpec(ns=RDF, lname='resource'),
                converter=lambda s: s.split('#')[1]),
        }
        if kw['type'] == 'Class':
            kw['subtype'] = 'module' \
                if subClassOf is not None \
                and subClassOf.attrib[qname(RDF, 'resource')] == \
                'http://www.w3.org/ns/dcat#Distribution' else 'component'
        return cls(**kw)

    def csvw_prop(self, lname: str) -> Any:
        """Returns the JSON value of a property in the CSVW namespace."""
        return _get(self.element, NameSpec(ns=CSVW, lname=lname), converter=json.loads)

    def to_column(self) -> Column:
        """Returns a csvw Column instance configured according to the term spec."""
        col = Column(
            name=self.csvw_prop('name') or self.element.find(qname(RDFS, 'label')).text,
            propertyUrl=self.element.attrib[qname(RDF, 'about')],
            datatype=self.csvw_prop('datatype') or 'string')
        for k in ['separator', 'null', 'valueUrl']:
            v = self.csvw_prop(k)
            if v:
                setattr(col, k, v)
        return col

    def comment(self, one_line=False) -> str:
        """
        Parse a text comment from the XML element of the ontology.
        """
        c = self.element.find("{http://www.w3.org/2000/01/rdf-schema#}comment")
        try:
            xml = ElementTree.tostring(c, default_namespace='http://www.w3.org/1999/xhtml')
        except (ValueError, TypeError):
            xml = ElementTree.tostring(c)
        # Turn the rdfs:comment element into a div, and strip namespace prefixes:
        res = re.sub(
            r'ns[0-9]+:comment(\s[^>]+)?',
            'div',
            xml.decode('utf8')
        ).replace('<html:', '<').replace('</html:', '</')
        return re.sub(r'\s+', ' ', res.replace('\n', ' ')) if one_line else res


TermDict = dict[str, Term]


class Terms(dict):
    """
    A dict of `Term`s keyed by local names.
    """
    def __init__(self, path: Optional[PathType] = None):
        self._path = path or pkg_path('terms.rdf')
        r = ElementTree.parse(str(self._path)).getroot()
        terms = [Term.from_element(e) for e in r.findall(qname(RDF, 'Property'))]
        for e in r.findall(qname(RDFS, 'Class')):
            terms.append(Term.from_element(e))
        dict.__init__(self, {t.name: t for t in terms})
        self.by_uri: TermDict = {t.uri: t for t in terms}

    def is_cldf_uri(self, uri: str) -> bool:
        """Whether the given URL is a CLDF Ontology term URI."""
        if uri and urllib.parse.urlparse(uri).netloc == 'cldf.clld.org':
            if uri not in self.by_uri:
                warnings.warn('If pycldf does not recognize valid CLDF URIs, You may be '
                              'running an outdated version. Please upgrade via '
                              '"pip install -U pycldf"')
                raise ValueError(uri)
            return True
        return False

    @property
    def properties(self) -> TermDict:  # pylint: disable=C0116
        return {k: v for k, v in self.items() if v.type == 'Property'}

    @property
    def classes(self) -> TermDict:  # pylint: disable=C0116
        return {k: v for k, v in self.items() if v.type == 'Class'}

    @property
    def modules(self) -> TermDict:  # pylint: disable=C0116
        return {k: v for k, v in self.items() if v.subtype == 'module'}

    @property
    def components(self) -> TermDict:  # pylint: disable=C0116
        return {k: v for k, v in self.items() if v.subtype == 'component'}


def term_uri(name: Union[Term, str], terms: Container[str] = None, ns: str = URL) -> Optional[str]:
    """
    Returns a full term URI associated with `name`.

    If `terms` are provided, we make sure the URI is contained in `terms`.
    """
    if isinstance(name, Term):
        return name.uri
    if not name.startswith(ns):  # So this may be a local name, i.e. the fragment of a term URI.
        sep = '' if ns.endswith('#') else '#'
        name = sep.join([ns, name])
    if not terms or name in terms:
        return name
    return None


TERMS = Terms()


def get_column_names(
        dataset: 'Dataset',
        use_component_names: bool = False,
        with_multiplicity: bool = False,
) -> types.SimpleNamespace:
    """
    Returns an object allowing programmatic access to the column names used for ontology terms
    in a specific dataset.

    .. code-block:: python

        >>> from pycldf import Dataset
        >>> from pycldf.terms import get_column_names
        >>> ds = Dataset.from_metadata('tests/data/ds1.csv-metadata.json')
        >>> res = get_column_names(ds, use_component_names=True)
        >>> res.ValueTable.languageReference
        'Language_ID'
    """
    comp_names = {
        k: k if use_component_names else k.replace('Table', '').lower() + 's'
        for k in TERMS.components}
    # Seed the result object with component names as attributes and None as value.
    name_map = types.SimpleNamespace(**{k: None for k in comp_names.values()})
    for term, attr_ in comp_names.items():
        table = dataset.get(term)
        if table:
            props = {}
            for k in TERMS.properties:  # Loop through properties in the ontology.
                col = dataset.get((table, k))
                if col:
                    if with_multiplicity:
                        props[k] = (col.name, bool(col.separator))
                    else:
                        props[k] = col.name
                else:
                    props[k] = None
            setattr(name_map, attr_, types.SimpleNamespace(**props))
    return name_map


def sniff(p: pathlib.Path) -> bool:
    """
    Determine whether a file contains CLDF metadata.

    :param p: `pathlib.Path` object for an existing file.
    :return: `True` if the file contains CLDF metadata, `False` otherwise.
    """
    if not p.is_file():  # pragma: no cover
        return False
    try:
        with p.open('rb') as fp:
            c = fp.read(10)
            try:
                c = c.decode('utf8').strip()
            except UnicodeDecodeError:
                return False
            if not c.startswith('{'):
                return False
    except (FileNotFoundError, OSError):  # pragma: no cover
        return False
    try:
        d = jsonlib.load(p)
    except json.decoder.JSONDecodeError:
        return False
    return d.get('dc:conformsTo', '').startswith(URL)
