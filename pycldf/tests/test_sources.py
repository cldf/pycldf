# coding: utf8
from __future__ import unicode_literals, print_function, division

from clldutils.testing import WithTempDir
from clldutils.path import Path


FIXTURES = Path(__file__).parent.joinpath('fixtures')
BIB = """@BOOK{Obrazy,
    author = {Borovský, Karel Havlíček},
    title = {Obrazy z Rus}
}

@BOOK{Elegie,
    author = {Borovský, Karel Havlíček},
    title = {Tirolské elegie}
}
"""


class Tests(WithTempDir):
    def test_sources(self):
        from pycldf.sources import Sources, Source

        src = Sources()
        src.add(BIB, Source('huber2005', author='Herrmann Huber', year='2005', title='y'))
        with self.assertRaises(ValueError):
            src.add(5)

        bib = self.tmp_path('test.bib')
        src.write(bib)

        src2 = Sources()
        src2.read(bib)

    def test_Source_persons(self):
        from pycldf.sources import Source

        self.assertEqual(len(list(Source.persons('A. Meier'))), 1)
        self.assertEqual(len(list(Source.persons('Meier, A.B.'))), 1)
        self.assertEqual(len(list(Source.persons('A. Meier, B. Meier, C.Meier'))), 3)

    def test_itersources(self):
        from pycldf.sources import itersources

        res = list(itersources('1234[1-4] ; maier2005'))
        self.assertEqual(res, [('1234', '1-4'), ('maier2005', None)])
