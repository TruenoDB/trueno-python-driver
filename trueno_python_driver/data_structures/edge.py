from .component import Component, ComponentType
from ..communication.message import Message
import logging
from promise import Promise
import json

__author__ = 'Miguel Rivera'
__version__ = '0.1.0'


class Edge(Component):
    """
    TruenoDB Edge primitive data structure.
    """
    def __init__(self):
        """
        Creates a new Edge instance.
        """
        super(self.__class__, self).__init__()

        self.__logger = logging.getLogger(__name__)

        self.type = ComponentType.Edge
        self.partition = None
        self.source = None
        self.target = None

    def vertices(self):
        """
        Fetch the vertices connected by this Edge.
        :return: Promise with source Vertex and target Vertex.
        """
        api_fun = 'ex_vertices'

        if not self.validate_graph_label():
            self.__logger.warning('Graph label is empty – ID: ' + self.id)
            return Promise.rejected('Graph label is empty – ID: ' + self.id)

        msg = Message()
        msg.payload['graph'] = self.parent_graph.label
        msg.payload['id'] = self.id

        self.__logger.debug(api_fun + ' – ' + json.dumps(msg.__dict__))
