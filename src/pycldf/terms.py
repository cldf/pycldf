import re
import json
import types
import warnings
import urllib.parse
from xml.etree import ElementTree

import attr
from csvw.metadata import Column

from pycldf.util import pkg_path

__all__ = ['term_uri', 'TERMS', 'get_column_names']

URL = 'http://cldf.clld.org/v1.0/terms.rdf'
RDF = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#'
RDFS = 'http://www.w3.org/2000/01/rdf-schema#'
CSVW = 'http://www.w3.org/ns/csvw#'
DC = 'http://purl.org/dc/terms/'


def term_uri(name, terms=None, ns=URL):
    if isinstance(name, Term):
        return name.uri
    if not name.startswith(ns):
        sep = '' if ns.endswith('#') else '#'
        name = sep.join([ns, name])
    if not terms or name in terms:
        return name
    return None


def qname(ns, lname):
    return '{%s}%s' % (ns, lname)


def _get(e, subelementns, subelementlname, attrns=None, attrlname=None, converter=None):
    """
    :return: Text content or attribute value of a subelement of e.
    """
    res = None
    subelement = e.find(qname(subelementns, subelementlname))
    if subelement is not None:
        if not attrlname:
            res = subelement.text
        else:
            res = subelement.attrib[qname(attrns, attrlname)]
    if converter and res:
        res = converter(res)
    return res


@attr.s
class Term(object):
    name = attr.ib()
    type = attr.ib(validator=attr.validators.in_(['Class', 'Property']))
    element = attr.ib()
    references = attr.ib(default=None)
    subtype = attr.ib(default=None)
    version = attr.ib(default=None, validator=attr.validators.matches_re(r'v[0-9]+(\.[0-9]+)+'))
    cardinality = attr.ib(
        default=None,
        validator=attr.validators.optional(attr.validators.in_(['singlevalued', 'multivalued'])))

    @property
    def uri(self):
        return '{0}#{1}'.format(URL, self.name)

    @classmethod
    def from_element(cls, e):
        subClassOf = e.find(qname(RDFS, 'subClassOf'))
        kw = dict(
            name=e.attrib[qname(RDF, 'about')].split('#')[1],
            version=_get(
                e, DC, 'hasVersion', RDF, 'resource',
                converter=lambda s: 'v' + s.split('/v')[1].replace('/', '')) or 'v1.0',
            type=e.tag.split('}')[1],
            element=e,
            cardinality=_get(e, DC, 'extent'),
            references=_get(
                e, DC, 'references', RDF, 'resource', converter=lambda s: s.split('#')[1]),
        )
        if kw['type'] == 'Class':
            kw['subtype'] = 'module' \
                if subClassOf is not None \
                and subClassOf.attrib[qname(RDF, 'resource')] == \
                'http://www.w3.org/ns/dcat#Distribution' else 'component'
        return cls(**kw)

    def csvw_prop(self, lname):
        return _get(self.element, CSVW, lname, converter=lambda s: json.loads(s))

    def to_column(self):
        col = Column(
            name=self.csvw_prop('name') or self.element.find(qname(RDFS, 'label')).text,
            propertyUrl=self.element.attrib[qname(RDF, 'about')],
            datatype=self.csvw_prop('datatype') or 'string')
        for k in ['separator', 'null', 'valueUrl']:
            v = self.csvw_prop(k)
            if v:
                setattr(col, k, v)
        return col

    def comment(self, one_line=False):
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


class Terms(dict):
    def __init__(self, path=None):
        self._path = path or pkg_path('terms.rdf')
        r = ElementTree.parse(str(self._path)).getroot()
        terms = [Term.from_element(e) for e in r.findall(qname(RDF, 'Property'))]
        for e in r.findall(qname(RDFS, 'Class')):
            terms.append(Term.from_element(e))
        dict.__init__(self, {t.name: t for t in terms})
        self.by_uri = {t.uri: t for t in terms}

    def is_cldf_uri(self, uri):
        if uri and urllib.parse.urlparse(uri).netloc == 'cldf.clld.org':
            if uri not in self.by_uri:
                warnings.warn('If pycldf does not recognize valid CLDF URIs, You may be '
                              'running an outdated version. Please upgrade via '
                              '"pip install -U pycldf"')
                raise ValueError(uri)
            return True
        return False

    @property
    def properties(self):
        return {k: v for k, v in self.items() if v.type == 'Property'}

    @property
    def classes(self):
        return {k: v for k, v in self.items() if v.type == 'Class'}

    @property
    def modules(self):
        return {k: v for k, v in self.items() if v.subtype == 'module'}

    @property
    def components(self):
        return {k: v for k, v in self.items() if v.subtype == 'component'}


TERMS = Terms()


def get_column_names(dataset, use_component_names=False, with_multiplicity=False):
    comp_names = {
        k: k if use_component_names else k.replace('Table', '').lower() + 's'
        for k in TERMS.components}
    name_map = types.SimpleNamespace(**{k: None for k in comp_names.values()})
    for term, attr_ in comp_names.items():
        try:
            table = dataset[term]
            props = {}
            for k in TERMS.properties:
                try:
                    col = dataset[table, k]
                    if with_multiplicity:
                        props[k] = (col.name, bool(col.separator))
                    else:
                        props[k] = col.name
                except KeyError:
                    props[k] = None
            setattr(name_map, attr_, types.SimpleNamespace(**props))
        except KeyError:
            pass
    return name_map
