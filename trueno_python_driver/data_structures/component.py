from ..communication.message import Message
from promise import Promise
from enum import Enum
import logging
import json
import uuid

__author__ = 'Miguel Rivera'
__version__ = '0.1.0'


class ComponentType(Enum):
    """
    Component Type Enumerator Class
    """
    Graph = 'g'
    Vertex = 'v'
    Edge = 'e'
    Compute = 'c'
    Empty = None

    @staticmethod
    def validate_cmp_type(cmp_type):
        if isinstance(cmp_type, type(ComponentType)):
            if cmp_type != ComponentType.Empty:
                return True
        return False


class Component:
    """
    Trueno Base Component Module.

    Defines shared methods and properties between the Trueno primitive data structures.
    """

    def __init__(self, identifier=uuid.uuid1().int >> 64, label='', properties=None, computed=None, meta=None):
        """
        Initialization of Component identifier (ID), attributes, computed fields and metadata fields
        :param identifier: ID for this Component
        :param properties: Attributes for this component as a Dictionary
        :param computed: Computed fields for this Component as a Dictionary
        :param meta: Metadata fields for this Component as a Dictionary
        """
        self.__logger = logging.getLogger(__name__)

        self.id = identifier
        self.label = label
        self.type = ComponentType.Empty
        if properties is None:
            self.properties = {}
        else:
            self.properties = properties
        if computed is None:
            self.computed = {}
        else:
            self.computed = computed
        if meta is None:
            self.meta = {}
        else:
            self.meta = meta
        self.parent_graph = ''

    def validate_graph_label(self):
        """
        Validates that the label of this Graph is defined.
        :return: True if label is present, False otherwise.
        """
        if self.parent_graph is not None:
            if self.parent_graph.label is not None:
                return True
        return False

    def persist(self):
        """
        Persist the new contents and/or changes in this Graph to the remote database.
        :return: Promise with the operation result.
        """
        api_fun = "ex_persist"

        if not self.validate_graph_label():
            self.__logger.warning('Graph Label not specified')
            return Promise.rejected('Graph Label not specified')

        if self.type.Edge:
            print('finish')

        msg = Message()
        msg.payload['graph'] = self.parent_graph.label
        msg.payload['type'] = self.type
        msg.payload['obj'] = json.dumps(self.__dict__)

        self.__logger.debug(api_fun + ' – ' + json.dumps(self.__dict__))

    def destroy(self, cmp_type=ComponentType.Empty, ftr=None):
        """
        Destroy a Component in the remote database.
        :param cmp_type: ComponentType of Component to be destroyed (Graph, Vertex, Edge).
        :param ftr: Filter to be applied to the destroy operation.
        :return: Promise with the operation result.
        """
        api_fun = 'ex_destroy'

        if not self.validate_graph_label():
            self.__logger.warning('Graph Label not specified')
            return Promise.rejected('Graph Label not specified')

        if self.type.Graph and cmp_type is not None:
            if not ComponentType.validate_cmp_type(cmp_type):
                self.__logger.warning('Invalid Component Type')
                return Promise.rejected('Invalid Component Type')

        msg = Message()
        msg.payload['graph'] = self.parent_graph.label

        if self.type.Graph:
            msg.payload['type'] = cmp_type
            msg.payload['ftr'] = ftr
        else:
            msg.payload['type'] = self.type

        msg.payload['obj']['id'] = self.id

        self.__logger.debug(api_fun + ' – ' + json.dumps(msg.__dict__))

        return self.parent_graph.connection.call(api_fun, msg)

