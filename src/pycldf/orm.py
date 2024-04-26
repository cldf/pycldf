"""
Object oriented (read-only) access to CLDF data

To read ORM objects from a `pycldf.Dataset`, there are two generic methods:

* :meth:`pycldf.Dataset.objects`
* :meth:`pycldf.Dataset.get_object`

Both will return default implementations of the objects, i.e. instances of the corresponding
class defined in this module. To customize these objects,

1. subclass the default and specify \
   the appropriate component (i.e. the table of the CLDF dataset which holds rows to be transformed\
   to this type):

   .. code-block:: python

      from pycldf.orm import Language

      class Variety(Language):
          __component__ = 'LanguageTable'

          def custom_method(self):
              pass

2. pass the class into the `objects` or `get_object` method.

In addition, module-specific subclasses of :class:`pycldf.Dataset` provide more meaningful
properties and methods, as shortcuts to the methods above. See
`<./dataset.html#subclasses-supporting-specific-cldf-modules>`_ for details.


Limitations:
------------
* We only support foreign key constraints for CLDF reference properties targeting either a \
  component's CLDF id or its primary key. This is because CSVW does not support unique constraints \
  other than the one implied by the primary key declaration.
* This functionality comes with the typical "more convenient API vs. less performance and bigger \
  memory footprint" trade-off. If you are running into problems with this, you might want to load \
  your data into a SQLite db using the `pycldf.db` module, and access via SQL. \
  Some numbers (to be interpreted relative to each other): \
  Reading ~400,000 rows from a ValueTable of a StructureDataset takes

  * ~2secs with csvcut, i.e. only making sure it's valid CSV
  * ~15secs iterating over ``pycldf.Dataset['ValueTable']``
  * ~35secs iterating over ``pycldf.Dataset.objects('ValueTable')``
"""
import types
import typing
import decimal
import functools
import collections

import csvw.metadata
from tabulate import tabulate

from pycldf.terms import TERMS, term_uri
from pycldf.util import DictTuple
from pycldf.sources import Reference

if typing.TYPE_CHECKING:
    from pycldf import Dataset  # pragma: no cover
    from pycldf.media import File  # pragma: no cover


def to_json(s):
    if isinstance(s, (list, tuple)):
        return [to_json(ss) for ss in s]
    if isinstance(s, dict):
        return {k: to_json(v) for k, v in s.items()}
    if isinstance(s, decimal.Decimal):
        return float(s)
    if s is None:
        return None
    if isinstance(s, (str, int, float, bool)):
        return s
    return str(s)


class Object:
    """
    Represents a row of a CLDF component table.

    Subclasses of `Object` are instantiated when calling `Dataset.objects` or `Dataset.get_object`.

    :ivar dataset: Reference to the `Dataset` instance, this object was loaded from.
    :ivar data: An `OrderedDict` with a copy of the row the object was instantiated with.
    :ivar cldf: A `dict` with CLDF-specified properties of the row, keyed with CLDF terms.
    :ivar id: The value of the CLDF id property of the row.
    :ivar name: The value of the CLDF name property of the row.
    :ivar description: The value of the CLDF description property of the row.
    :ivar pk: The value of the column specified as primary key for the table. (May differ from id)
    """
    # If a subclass name can not be used to derive the CLDF component name, the component can be
    # specified here:
    __component__ = None

    def __init__(self, dataset: 'Dataset', row: dict):
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
        self.cldf = types.SimpleNamespace(**self.cldf)
        self.dataset = dataset
        self.id = self.cldf.id
        self.pk = None
        t = dataset[self.component_name()]
        if t.tableSchema.primaryKey and len(t.tableSchema.primaryKey) == 1:
            self.pk = self.data[dataset[self.component_name()].tableSchema.primaryKey[0]]
        self.name = getattr(self.cldf, 'name', None)
        self.description = getattr(self.cldf, 'name', None)

    def __repr__(self):
        return '<{}.{} id="{}">'.format(self.__class__.__module__, self.__class__.__name__, self.id)

    @classmethod
    def component_name(cls) -> str:
        return cls.__component__ or (cls.__name__ + 'Table')

    @property
    def component(self) -> str:
        """
        Name of the CLDF component the object belongs to. Can be used to lookup the corresponding \
        table via `obj.dataset[obj.component_name()]`.
        """
        return self.__class__.component_name()

    @property
    def key(self) -> typing.Tuple[int, str, str]:
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

    def aboutUrl(self, col='id') -> typing.Union[str, None]:
        """
        The table's `aboutUrl` property, expanded with the object's row as context.
        """
        return self._expand_uritemplate('aboutUrl', col)

    def valueUrl(self, col='id') -> typing.Union[str, None]:
        """
        The table's `valueUrl` property, expanded with the object's row as context.
        """
        return self._expand_uritemplate('valueUrl', col)

    def propertyUrl(self, col='id') -> typing.Union[str, None]:
        """
        The table's `propertyUrl` property, expanded with the object's row as context.
        """
        return self._expand_uritemplate('propertyUrl', col)

    @functools.cached_property
    def references(self) -> typing.Tuple[Reference]:
        """
        `pycldf.Reference` instances associated with the object.

        >>> obj.references[0].source['title']
        >>> obj.references[0].fields.title
        >>> obj.references[0].description  # The "context", typically cited pages
        """
        return DictTuple(
            self.dataset.sources.expand_refs(getattr(self.cldf, 'source', []) or []),
            key=lambda r: r.source.id,
            multi=True,
        )

    def related(self, relation: str) -> typing.Union[None, 'Object']:
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
            ref = self.dataset.get_foreign_key_reference(self.component_name(), relation)
            if ref:
                if str(ref[1].propertyUrl) == term_uri('id'):
                    return self.dataset.get_object(TERMS[relation].references, fk)
                if [ref[1].name] == self.dataset[TERMS[relation].references].tableSchema.primaryKey:
                    return self.dataset.get_object(TERMS[relation].references, fk, pk=True)
                raise NotImplementedError('pycldf does not support foreign key constraints '
                                          'referencing columns other than CLDF id or primary key.')

    def all_related(self, relation: str) -> typing.Union[DictTuple, list]:
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


class _WithLanguageMixin:
    @property
    def language(self):
        return self.related('languageReference')

    @property
    def languages(self):
        return self.all_related('languageReference')


class _WithParameterMixin:
    @functools.cached_property
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


class Code(Object, _WithParameterMixin):
    pass


class Cognateset(Object):
    @property
    def cognates(self):
        return DictTuple(v for v in self.dataset.objects('CognateTable') if v.cognateset == self)


class Cognate(Object):
    @property
    def form(self):
        return self.related('formReference')

    @property
    def cognateset(self):
        return self.related('cognatesetReference')


class Contribution(Object):
    @property
    def sentences(self):
        res = []
        if self.dataset.module == 'TextCorpus':
            # Return the list of lines, ordered by position.
            for e in self.dataset.objects('ExampleTable'):
                if e.cldf.contributionReference == self.id:
                    if not getattr(e.cldf, 'exampleReference', None):
                        # Not just an alternative translation line.
                        res.append(e)
        if res and hasattr(res[0].cldf, 'position'):
            return sorted(res, key=lambda e: getattr(e.cldf, 'position'))
        return res


class Entry(Object, _WithLanguageMixin):
    @property
    def senses(self):
        return DictTuple(v for v in self.dataset.objects('SenseTable') if self in v.entries)


class Example(Object, _WithLanguageMixin):
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

    @property
    def text(self):
        """
        Examples in a TextCorpus are interpreted as lines of text.
        """
        if self.dataset.module == 'TextCorpus' and hasattr(self.cldf, 'contributionReference'):
            return self.related('contributionReference')

    @property
    def alternative_translations(self):
        res = []
        if hasattr(self.cldf, 'exampleReference'):
            # There's a self-referential foreign key. We assume this to link together full examples
            # and alternative translations.
            for ex in self.dataset.objects('ExampleTable'):
                if ex.cldf.exampleReference == self.id:
                    res.append(ex)
        return res


class Form(Object, _WithLanguageMixin, _WithParameterMixin):
    pass


class FunctionalEquivalentset(Object):
    pass


class FunctionalEquivalent(Object):
    @property
    def form(self):  # pragma: no cover
        return self.related('formReference')


class Language(Object):
    """
    Language objects correspond to rows in a dataset's ``LanguageTable``.

    Language objects provide easy access to somewhat complex derivatives of the dataset's info
    on the language, e.g. its speaker area as GeoJSON object.

    .. code-block:: python

        >>> from pycldf import Dataset
        >>> ds = Dataset.from_metadata('tests/data/dataset_with_media/metadata.json')
        >>> lg = ds.get_object('LanguageTable', '1')
        >>> lg.speaker_area_as_geojson_feature['geometry']['type']
        'MultiPolygon'
    """
    @property
    def lonlat(self) -> typing.Union[None, typing.Tuple[decimal.Decimal]]:
        """
        :return: (longitude, latitude) pair if coordinates are defined, else `None`.
        """
        if hasattr(self.cldf, 'latitude'):
            return (self.cldf.longitude, self.cldf.latitude)

    @property
    def as_geojson_feature(self) -> typing.Union[None, typing.Dict[str, typing.Any]]:
        """
        `dict` suitable for serialization as GeoJSON Feature object, with the point coordinate as
        geographic data.

        .. seealso:: https://datatracker.ietf.org/doc/html/rfc7946#section-3.2
        """
        if self.lonlat:
            return to_json({
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": self.lonlat},
                "properties": vars(self.cldf),
            })

    @functools.cached_property
    def speaker_area(self) -> typing.Union[None, 'File']:
        """
        A `pycldf.media.File` object containing information about the speaker area of the language.
        """
        from pycldf.media import File

        if getattr(self.cldf, 'speakerArea', None):
            return File.from_dataset(self.dataset, self.related('speakerArea'))

    @functools.cached_property
    def speaker_area_as_geojson_feature(self) -> typing.Union[None, typing.Dict[str, typing.Any]]:
        """
        `dict` suitable for serialization as GeoJSON Feature object, with a speaker area Polygon
        or MultiPolygon as geographic data.

        .. seealso:: https://datatracker.ietf.org/doc/html/rfc7946#section-3.2
        """
        if self.speaker_area and self.speaker_area.mimetype.subtype == 'geo+json':
            res = self.speaker_area.read_json()
            if res['type'] == 'FeatureCollection':
                for feature in res['features']:
                    if feature['properties']['cldf:languageReference'] == self.id:
                        return feature
            else:
                assert res['type'] == 'Feature'
                return res

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


class Media(Object):
    @property
    def downloadUrl(self):
        if hasattr(self.cldf, 'downloadUrl'):
            return self.cldf.downloadUrl
        return self.valueUrl()


class ParameterNetworkEdge(Object):
    __component__ = 'ParameterNetwork'


class Parameter(Object):
    @functools.cached_property
    def columnSpec(self):
        columnSpec = getattr(self.cldf, 'columnSpec', None)
        if columnSpec:
            return csvw.metadata.Column.fromvalue(columnSpec)

    @functools.cached_property
    def datatype(self):
        if 'datatype' in self.data \
                and self.dataset['ParameterTable', 'datatype'].datatype.base == 'json':
            if self.data['datatype']:
                return csvw.metadata.Datatype.fromvalue(self.data['datatype'])

    @property
    def codes(self):
        return DictTuple(v for v in self.dataset.objects('CodeTable') if v.parameter == self)

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


class Tree(Object):
    pass


class Value(Object, _WithLanguageMixin, _WithParameterMixin):
    """
    Value objects correspond to rows in a dataset's ``ValueTable``.

    While a Value's string representation is typically available from the ``value`` column,
    i.e. as ``Value.cldf.value``, The interpretation of this value may be dictated by other
    metadata.

    - Categorical data will often describe possible values (aka "codes") using a ``CodeTable``.
      In this case, the associated ``Code`` object of a ``Value`` is available as ``Value.code``.
    - Typed data may use a ``columnSpec`` property in ``ParameterTable`` to specify how to read
      the string value.

    .. code-block:: python

        >>> from csvw.metadata import Column
        >>> from pycldf import StructureDataset
        >>> cs = Column.fromvalue(dict(datatype=dict(base='integer', maximum=5), separator=' '))
        >>> ds = StructureDataset.in_dir('.')
        >>> ds.add_component('ParameterTable')
        >>> ds.write(
        ...     ParameterTable=[dict(ID='1', ColumnSpec=cs.asdict())],
        ...     ValueTable=[dict(ID='1', Language_ID='l', Parameter_ID='1', Value='1 2 3')],
        ... )
        >>> v = ds.objects('ValueTable')[0]
        >>> v.cldf.value
        '1 2 3'
        >>> v.typed_value
        [1, 2, 3]
    """
    @property
    def typed_value(self):
        if self.parameter.columnSpec:
            return self.parameter.columnSpec.read(self.cldf.value)
        if self.parameter.datatype:
            return self.parameter.datatype.read(self.cldf.value)
        return self.cldf.value

    @property
    def code(self):
        return self.related('codeReference')

    @property
    def examples(self):
        return self.all_related('exampleReference')
