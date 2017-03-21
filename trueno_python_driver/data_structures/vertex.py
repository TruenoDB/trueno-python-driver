from .component import Component, ComponentType
from ..communication.message import Message
from promise import Promise
import logging
import json

__author__ = 'Miguel Rivera'
__version__ = '0.1.0'


class Vertex(Component):
    """
    TruenoDB Vertex primitive data structure.
    """
    def __init__(self, partition=0):
        """
        Creates a new Vertex instance.
        :param partition: Number of partitions the Vertex spans.
        """
        super(self.__class__, self).__init__()

        self.__logger = logging.getLogger(__name__)

        self.partition = partition
        self.type = ComponentType.Vertex

    def in_neighbors(self, cmp_type, ftr):
        """
        Get the Vertices/Edges pointing to this Vertex.
        :param cmp_type: Neighbors ComponentType (Vertex or Edge).
        :param ftr: Filter to be applied to the neighbors search.
        :return: List of Vertices or Edges.
        """
        return self.__neighbors(cmp_type, ftr, 'in')

    def out_neighbors(self, cmp_type, ftr):
        """
        Get the Vertices/Edges this Vertex points to.
        :param cmp_type: Neighbors ComponentType (Vertex or Edge).
        :param ftr: Filter to be applied to the neighbors search.
        :return: List of Vertices or Edges.
        """
        return self.__neighbors(cmp_type, ftr, 'out')

    def __neighbors(self, cmp_type, ftr, direction):
        """
        Performs the neighbors search.
        :param cmp_type: Neighbors ComponentType (Vertex or Edge).
        :param ftr: Filter to be applied to the neighbors search.
        :param direction: Vertices/Edges pointing in or out of this Vertex.
        :return: List of Vertices or Edges.
        """
        api_fun = 'ex_neighbors'

        if not ComponentType.validate_cmp_type(cmp_type):
            self.__logger.warning('Invalid Component Type – ID: ' + self.id)
            return Promise.rejected('Invalid Component Type – ID: ' + self.id)

        if not self.validate_graph_label():
            self.__logger.warning('Graph label is empty – ID: ' + self.id)
            return Promise.rejected('Graph label is empty – ID: ' + self.id)

        msg = Message()
        msg.payload['graph'] = self.parent_graph.label
        msg.payload['cmp'] = cmp_type
        msg.payload['id'] = self.id
        msg.payload['dir'] = direction

        if ftr is not None:
            msg.payload['ftr'] = ftr

        self.__logger.debug(api_fun + ' – ' + json.dumps(msg.__dict__))

        return self.parent_graph.connection.call(api_fun, msg).then(lambda res: Promise.resolve(list(res)))

    def in_degree(self, cmp_type, ftr):
        """
        Get the degree of the Vertices/Edges pointing to this Vertex.
        :param cmp_type: Neighbors ComponentType (Vertex or Edge).
        :param ftr: Filter to be applied to the neighbors search.
        :return: List of Vertices or Edges.
        """
        return self.__neighbors(cmp_type, ftr, 'in')

    def out_degree(self, cmp_type, ftr):
        """
        Get the degree of the Vertices/Edges this Vertex points to.
        :param cmp_type: Neighbors ComponentType (Vertex or Edge).
        :param ftr: Filter to be applied to the neighbors search.
        :return: List of Vertices or Edges.
        """
        return self.__neighbors(cmp_type, ftr, 'out')

    def __degree(self, cmp_type, ftr, direction):
        """
        Performs the degree search in this Vertex neighbors.
        :param cmp_type: Neighbors ComponentType (Vertex or Edge).
        :param ftr: Filter to be applied to the neighbors search.
        :param direction: Vertices/Edges pointing in or out of this Vertex.
        :return: List of Vertices or Edges.
        """
        api_fun = 'ex_degree'

        if not ComponentType.validate_cmp_type(cmp_type):
            self.__logger.warning('Invalid Component Type – ID: ' + self.id)
            return Promise.rejected('Invalid Component Type – ID: ' + self.id)

        if not self.validate_graph_label():
            self.__logger.warning('Graph label is empty – ID: ' + self.id)
            return Promise.rejected('Graph label is empty – ID: ' + self.id)

        msg = Message()
        msg.payload['graph'] = self.parent_graph.label
        msg.payload['cmp'] = cmp_type
        msg.payload['id'] = self.id
        msg.payload['dir'] = direction

        if ftr is not None:
            msg.payload['ftr'] = ftr

        self.__logger.debug(api_fun + ' – ' + json.dumps(msg.__dict__))

        return self.parent_graph.connection.call(api_fun, msg)
