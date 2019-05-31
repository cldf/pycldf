# coding: utf8
from __future__ import unicode_literals, print_function, division

from xml.etree import ElementTree
from json import loads

from six.moves.urllib.parse import urlparse

import attr
from csvw.metadata import Column

from pycldf.util import pkg_path

__all__ = ['term_uri', 'TERMS']

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


@attr.s
class Term(object):
    name = attr.ib()
    type = attr.ib(validator=attr.validators.in_(['Class', 'Property']))
    element = attr.ib()
    references = attr.ib(default=None)

    @property
    def uri(self):
        return '{0}#{1}'.format(URL, self.name)

    @classmethod
    def from_element(cls, e):
        ref = e.find(qname(DC, 'references'))
        return cls(
            name=e.attrib[qname(RDF, 'about')].split('#')[1],
            type=e.tag.split('}')[1],
            element=e,
            references=ref.attrib[qname(RDF, 'resource')].split('#')[1]
            if ref is not None else None)

    def csvw_prop(self, lname):
        if self.element.find(qname(CSVW, lname)) is not None:
            return loads(self.element.find(qname(CSVW, lname)).text)

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


class Terms(dict):
    def __init__(self):
        r = ElementTree.parse(pkg_path('terms.rdf').as_posix()).getroot()
        terms = [Term.from_element(e) for e in r.findall(qname(RDF, 'Property'))]
        for e in r.findall(qname(RDFS, 'Class')):
            terms.append(Term.from_element(e))
        dict.__init__(self, {t.name: t for t in terms})
        self.by_uri = {t.uri: t for t in terms}

    def is_cldf_uri(self, uri):
        if uri and urlparse(uri).netloc == 'cldf.clld.org':
            if uri not in self.by_uri:
                raise ValueError(uri)
            return True
        return False

    @property
    def properties(self):
        return {k: v for k, v in self.items() if v.type == 'Property'}

    @property
    def classes(self):
        return {k: v for k, v in self.items() if v.type == 'Class'}


TERMS = Terms()
