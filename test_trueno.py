import unittest
import logging
from trueno_python_driver import Trueno
from trueno_python_driver.data_structures.component import ComponentType
from promise import Promise


class TruenoTests(unittest.TestCase):
    trueno = Trueno(auto_connect=False)
    logger = logging.getLogger(__name__)

    @classmethod
    def setUpClass(cls):
        cls.trueno.connect(conn_callback, disc_callback)
        cls.assertTrue(cls, cls.trueno.connected())

        trueno = cls.trueno.graph('trueno')

        trueno.properties = {'version': '1'}

        trueno.create().then(lambda res: cls.assertTrue(cls, res), lambda err: cls.fail(err))

    @classmethod
    def tearDownClass(cls):
        trueno = cls.trueno.graph('trueno')

        trueno.destroy(ComponentType.Graph).then(lambda res: cls.assertTrue(cls, res),
                                                 lambda err: cls.fail(err))

        cls.trueno.disconnect()

        cls.assertTrue(cls, cls.trueno.connected())

    def test_create_vertices(self):
        trueno = self.trueno.graph('trueno')

        v1 = trueno.add_vertex()
        v2 = trueno.add_vertex()
        v3 = trueno.add_vertex()
        v4 = trueno.add_vertex()
        v5 = trueno.add_vertex()
        v6 = trueno.add_vertex()

        v1.setId(1)
        v2.setId(2)
        v3.setId(3)
        v4.setId(4)
        v5.setId(5)
        v6.setId(6)

        v1.setProperty('name', 'victor')
        v1.setProperty('age', '25')

        v2.setProperty('name', 'servio')
        v2.setProperty('age', '30')

        v3.setProperty('name', 'edgardo')
        v3.setProperty('age', '35')

        v4.setProperty('name', 'miguel')
        v4.setProperty('age', '20')

        v5.setProperty('name', 'peng')
        v5.setProperty('age', '25')

        v6.setProperty('name', 'chi')
        v6.setProperty('age', '20')

        v6.setProperty('name', 'grama')
        v6.setProperty('age', '30')

        return Promise.all([
            v1.persist(),
            v2.persist(),
            v3.persist(),
            v4.persist(),
            v5.persist(),
            v6.persist()]).then(lambda res: (self.assertEqual(res, ['1', '2', '3', '4', '5', '6'])),
                                lambda res: self.fail(res))


if __name__ == '__main__':
    unittest.main()


def conn_callback(*args):
    print('Connected')


def disc_callback(*args):
    print('Disconnected')