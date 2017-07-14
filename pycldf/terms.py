# coding: utf8
from __future__ import unicode_literals, print_function, division
from xml.etree import ElementTree

import attr

from pycldf.util import pkg_path

URL = "http://cldf.clld.org/v1.0/terms.rdf"
RDF = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
RDFS = "http://www.w3.org/2000/01/rdf-schema#"


def term_uri(name, ns=URL):
    if not name.startswith(ns):
        sep = '' if ns.endswith('#') else '#'
        name = sep.join([ns, name])
    return name


def qname(ns, lname):
    return '{%s}%s' % (ns, lname)


@attr.s
class Term(object):
    name = attr.ib()
    label = attr.ib()
    type = attr.ib(validator=attr.validators.in_(['Class', 'Property']))

    @property
    def uri(self):
        return term_uri(self.name)

    @classmethod
    def from_element(cls, e):
        return cls(
            name=e.attrib[qname(RDF, 'about')].split('#')[1],
            label=e.find(qname(RDFS, 'label')).text,
            type=e.tag.split('}')[1])


class Terms(dict):
    def __init__(self):
        r = ElementTree.parse(pkg_path('terms.rdf').as_posix()).getroot()
        terms = [Term.from_element(e) for e in r.findall(qname(RDF, 'Property'))]
        for e in r.findall(qname(RDFS, 'Class')):
            terms.append(Term.from_element(e))
        dict.__init__(self, {t.name: t for t in terms})

    @property
    def properties(self):
        return {k: v for k, v in self.items() if v.type == 'Property'}

    @property
    def classes(self):
        return {k: v for k, v in self.items() if v.type == 'Class'}


TERMS = Terms()
