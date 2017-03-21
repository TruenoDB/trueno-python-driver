from __future__ import print_function, absolute_import
from .component import Component
from .component import ComponentType
from .vertex import Vertex
from .edge import Edge
from .filter import Filter
from ..communication.message import Message
import logging
from promise import Promise
import json
import inspect

__author__ = 'Miguel Rivera'
__version__ = '0.1.0'


class Graph(Component):
    """
    TruenoDB Graph primitive data structure.
    """

    def __init__(self):
        """
        Creates a new Graph instance.
        """
        super(self.__class__, self).__init__()

        self.__logger = logging.getLogger(__name__)

        self.parent_graph = self
        self.type = ComponentType.Graph
        self.edges = {}
        self.vertices = {}
        self.bulk = False
        self.bulkOperations = []
        self.connection = None
        self.filter = Filter()

    def add_vertex(self):
        """
        Add a new Vertex to this Graph.
        :return: created Vertex.
        """
        v = Vertex()
        v.parent_graph = self
        self.vertices[v.id] = v
        return v

    def add_edge(self, source='', target=''):
        """
        Add a new Edge to this Graph.
        :param source: Vertex source of this Edge.
        :param target: Vertex target of this Edge.
        :return: created Edge.
        """
        e = Edge()

        if source is not '':
            e.source = source

        if target is not '':
            e.target = target

        e.parent_graph = self
        self.edges[e.id] = e
        return e

    def fetch(self, cmp_type=ComponentType.Empty, ftr=None):
        """
        Retrieves this Graph or a part of this Graph from the TruenoDB database.
        :param cmp_type: ComponentType to fetch – Vertices and Edges, Vertices only, Edges only.
        :param ftr: Filter applied on the fetch operation.
        :return: Promise with Graph contents.
        """
        api_fun = 'ex_fetch'

        if not ComponentType.validate_cmp_type(cmp_type):
            self.__logger.warning('Invalid Component Type – ID: ' + self.id)
            return Promise.rejected('Invalid Component Type – ID: ' + self.id)

        if not self.validate_graph_label():
            self.__logger.warning('Graph label is empty – ID: ' + self.id)
            return Promise.rejected('Graph label is empty – ID: ' + self.id)

        msg = Message()
        msg.payload['graph'] = self.label
        msg.payload['type'] = cmp_type

        if ftr is not None:
            msg.payload['ftr'] = ftr

        self.__logger.debug(api_fun + ' – ' + json.dumps(msg.__dict__))

        return self.connection.call(api_fun, msg)

    def open(self):
        """
        Open a Graph in the remote database.
        :return: Promise with operation result.
        """
        api_fun = 'ex_open'

        if not self.validate_graph_label():
            self.__logger.warning('Graph label is empty – ID: ' + self.id)
            return Promise.rejected('Graph label is empty – ID: ' + self.id)

        msg = Message()
        msg.payload['graph'] = self.label
        msg.payload['type'] = ComponentType.Graph.value
        msg.payload['mask'] = True
        msg.payload['obj'] = json.dumps(self.__dict__)

        self.__logger.debug(api_fun + ' – ' + json.dumps(msg.__dict__))

        return self.connection.call(api_fun, msg)

    def count(self, cmp_type, ftr):
        """
        Count components at the remote database.
        :param cmp_type: ComponentType to count.
        :param ftr: Filter applied to the count operation.
        :return: Promise with remote count result.
        """
        api_fun = 'ex_count'

        if not ComponentType.validate_cmp_type(cmp_type):
            self.__logger.warning('Invalid Component Type – ID: ' + self.id)
            return Promise.rejected('Invalid Component Type – ID: ' + self.id)

        if not self.validate_graph_label():
            self.__logger.warning('Graph label is empty – ID: ' + self.id)
            return Promise.rejected('Graph label is empty – ID: ' + self.id)

        msg = Message()
        msg.payload['graph'] = self.label
        msg.payload['type'] = ComponentType.Graph

        if ftr is not None:
            msg.payload['ftr'] = ftr

        self.__logger.debug(api_fun + ' – ' + json.dumps(msg.__dict__))

        return self.connection.call(api_fun, msg)

    def create(self):
        """
        Create a new Graph at the remote database.
        :return: Promise with operation result.
        """
        api_fun = 'ex_create'

        if not self.validate_graph_label():
            self.__logger.warning('Graph label is empty – ID: ' + self.id)
            return Promise.rejected('Graph label is empty – ID: ' + self.id)

        msg = Message()
        msg.payload['graph'] = self.label
        msg.payload['type'] = ComponentType.Graph.value
        msg.payload['obj'] = str(dict((k, v)) for k, v in inspect.getmembers(self) if not k.startswith('_')
                                 and not inspect.isabstract(v)
                                 and not inspect.isbuiltin(v)
                                 and not inspect.isfunction(v)
                                 and not inspect.isgenerator(v)
                                 and not inspect.isgeneratorfunction(v)
                                 and not inspect.ismethod(v)
                                 and not inspect.ismethoddescriptor(v)
                                 and not inspect.isroutine(v)
                                 )

        self.__logger.debug(api_fun + ' – ' + str(msg.__dict__))

        return self.connection.call(api_fun, msg.__dict__)

    def bulk(self):
        """
        Perform Graph operations in batch mode.
        :return: Promise with operation result.
        """
        api_fun = 'ex_bulk'

        if not self.validate_graph_label():
            self.__logger.warning('Graph label is empty – ID: ' + self.id)
            return Promise.rejected('Graph label is empty – ID: ' + self.id)

        msg = Message()
        msg.payload['graph'] = self.label
        msg.payload['operations'] = self.bulkOperations

        if len(self.bulkOperations) > 0:
            res = {'took': 0, 'errors': False, 'items': []}
            return Promise.resolve(res)

        self.__logger.debug(api_fun + ' – ' + json.dumps(msg.__dict__))

        return self.connection.call(api_fun, msg).then(self.__bulk_resolved)

    def __bulk_resolved(self):
        """
        Callback function executed after bulk operation completes.
        Clears internal data structures.
        """
        self.bulk = False
        self.bulkOperations = []
