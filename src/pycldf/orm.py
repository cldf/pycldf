"""
Object oriented (read-only) access to CLDF data

To read ORM objects from a `pycldf.Dataset`, use two methods
- `pycldf.Dataset.objects`
- `pycldf.Dataset.get_object`

Both will return default implementations of the objects, i.e. instances of the corresponding
class defined in this module. To customize these objects,
- subclass the default and specify
  the appropriate component (i.e. the table of the CLDF dataset which holds rows to be transformed
  to this type):

  ```python
  from pycldf.orm import Language

  class Variety(Language):
      __component__ = 'LanguageTable'

      def custom_method(self):
          pass
  ```
- pass the class into the `objects` or `get_object` method.

This functionality comes with the typical "more convenient API vs. less performance and bigger
memory footprint" trade-off. If you are running into problems with this, you might want to load
your data into a SQLite db using the `pycldf.db` module, and access via SQL.
"""
import argparse
import collections

from tabulate import tabulate
from clldutils.misc import lazyproperty

from pycldf.terms import TERMS
from pycldf.util import DictTuple


class Object:
    """
    Represents a row of a CLDF component table.
    """
    # If a subclass name can not be used to derive the CLDF component name, the component can be
    # specified here:
    __component__ = None

    def __init__(self, dataset, row):
        # Get a mapping of column names to pairs (CLDF property name, list-valued) for columns
        # present in the component specified by class name.
        cldf_cols = {
            v[0]: (k, v[1])
            for k, v in vars(getattr(dataset.readonly_column_names, self.component)).items()
            if v}
        self._listvalued = set(v[0] for v in cldf_cols.values() if v[1])
        self.cldf = {}
        self.data = collections.OrderedDict()
        for k, v in row.items():
            # We go through the items of the row and slot them into the appropriate bags:
            self.data[k] = v
            if k in cldf_cols:
                self.cldf[cldf_cols[k][0]] = v
        # Make cldf properties accessible as attributes:
        self.cldf = argparse.Namespace(**self.cldf)
        self.dataset = dataset
        self.id = self.cldf.id
        self.name = getattr(self.cldf, 'name', None)
        self.description = getattr(self.cldf, 'name', None)

    @classmethod
    def component_name(cls):
        return cls.__component__ or (cls.__name__ + 'Table')

    @property
    def component(self):
        return self.__class__.component_name()

    @property
    def key(self):
        return id(self.dataset), self.__class__.__name__, self.id

    def __hash__(self):
        return hash(self.key)

    def __eq__(self, other):
        if isinstance(self, Object):
            return self.key == other.key
        return NotImplemented  # pragma: no cover

    def _expand_uritemplate(self, attr, col):
        """
        CSVW cells can specify various URI templates which must be expanded supplying the full
        row as context. Thus, expansion is available as method on this row object.
        """
        col = self.dataset[self.component, col]
        variables = {k: v for k, v in vars(self.cldf).items()}
        variables.update(self.data)
        if getattr(col, attr, None):
            return getattr(col, attr).expand(**variables)

    def aboutUrl(self, col='id'):
        return self._expand_uritemplate('aboutUrl', col)

    def valueUrl(self, col='id'):
        return self._expand_uritemplate('valueUrl', col)

    def propertyUrl(self, col='id'):
        return self._expand_uritemplate('propertyUrl', col)

    @lazyproperty
    def references(self):
        """
        >>> obj.references[0].source['title']
        >>> obj.references[0].fields.title
        >>> obj.references[0].description  # The "context", typically cited pages

        :return: `list` of `pycldf.sources.Reference` objects.
        """
        return DictTuple(
            self.dataset.sources.expand_refs(getattr(self.cldf, 'source', []) or []),
            key=lambda r: r.source.id,
            multi=True,
        )

    def related(self, relation):
        """
        The CLDF ontology specifies several "reference properties". This method returns the first
        related object specified by such a property.

        :param relation: a CLDF reference property name.
        :return: related `Object` instance.
        """
        if relation in self._listvalued:
            raise ValueError(
                '{} is list-valued, use `all_related` to retrieve related objects'.format(relation))
        fk = getattr(self.cldf, relation, None)
        if fk:
            return self.dataset.get_object(TERMS[relation].references, fk)

    def all_related(self, relation):
        """
        CLDF reference properties can be list-valued. This method returns all related objects for
        such a property.
        """
        fks = getattr(self.cldf, relation, None)
        if fks and not isinstance(fks, list):
            fks = [fks]
        if fks:
            return DictTuple(self.dataset.get_object(TERMS[relation].references, fk) for fk in fks)
        return []


class WithLanguageMixin:
    @property
    def language(self):
        return self.related('languageReference')

    @property
    def languages(self):
        return self.all_related('languageReference')


class WithParameterMixin:
    @property
    def parameter(self):
        return self.related('parameterReference')

    @property
    def parameters(self):
        return self.all_related('parameterReference')


class Borrowing(Object):
    @property
    def targetForm(self):
        return self.related('targetFormReference')

    @property
    def sourceForm(self):
        return self.related('sourceFormReference')


class Code(Object, WithParameterMixin):
    pass


class Cognateset(Object):
    pass


class Cognate(Object):
    @property
    def form(self):
        return self.related('formReference')

    @property
    def cognateset(self):
        return self.related('cognatesetReference')


class Entry(Object, WithLanguageMixin):
    @property
    def senses(self):
        return DictTuple(v for v in self.dataset.objects('SenseTable') if self in v.entries)


class Example(Object, WithLanguageMixin):
    @property
    def metaLanguage(self):
        return self.related('metaLanguageReference')

    @property
    def igt(self):
        return '{0}\n{1}\n{2}'.format(
            self.cldf.primaryText,
            tabulate([self.cldf.gloss], self.cldf.analyzedWord, tablefmt='plain'),
            self.cldf.translatedText,
        )


class Form(Object, WithLanguageMixin, WithParameterMixin):
    pass


class FunctionalEquivalentset(Object):
    pass


class FunctionalEquivalent(Object):
    @property
    def form(self):  # pragma: no cover
        return self.related('formReference')


class Language(Object):
    @property
    def lonlat(self):
        """
        :return: (longitude, latitude) pair
        """
        if hasattr(self.cldf, 'latitude'):
            return (self.cldf.longitude, self.cldf.latitude)

    @property
    def as_geojson_feature(self):
        if self.lonlat:
            return {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": list(self.lonlat)},
                "properties": self.cldf,
            }

    @property
    def values(self):
        return DictTuple(v for v in self.dataset.objects('ValueTable') if self in v.languages)

    @property
    def forms(self):
        return DictTuple(v for v in self.dataset.objects('FormTable') if self in v.languages)

    def glottolog_languoid(self, glottolog_api):
        """
        Get a Glottolog languoid associated with the `Language`.

        :param glottolog_api: `pyglottolog.Glottolog` instance or `dict` mapping glottocodes to \
        `pyglottolog.langoids.Languoid` instances.
        :return: `pyglottolog.langoids.Languoid` instance or `None`.
        """
        if isinstance(glottolog_api, dict):
            return glottolog_api.get(self.cldf.glottocode)
        return glottolog_api.languoid(self.cldf.glottocode)


class Parameter(Object):
    @property
    def values(self):
        return DictTuple(v for v in self.dataset.objects('ValueTable') if self in v.parameters)

    @property
    def forms(self):
        return DictTuple(v for v in self.dataset.objects('FormTable') if self in v.parameters)

    def concepticon_conceptset(self, concepticon_api):
        """
        Get a Concepticon conceptset associated with the `Parameter`.

        :param concepticon_api: `pyconcepticon.Concepticon` instance or `dict` mapping conceptset \
        IDs to `pyconcepticon.models.Conceptset` instances.
        :return: `pyconcepticon.models.Conceptset` instance or `None`.
        """
        if isinstance(concepticon_api, dict):
            return concepticon_api.get(self.cldf.concepticonReference)
        return concepticon_api.conceptsets.get(self.cldf.concepticonReference)


class Sense(Object):
    @property
    def entry(self):
        return self.related('entryReference')

    @property
    def entries(self):
        return self.all_related('entryReference')


class Value(Object, WithLanguageMixin, WithParameterMixin):
    @property
    def code(self):
        return self.related('codeReference')

    @property
    def examples(self):
        return self.all_related('exampleReference')
