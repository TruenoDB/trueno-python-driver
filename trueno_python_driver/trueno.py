import json
import logging

from .communication import RPC, Message
from .data_structures.graph import Graph


class Trueno:
    """
    TruenoDB Python Driver – Provides interaction with a remote Trueno Database
    """
    def __init__(self, host='http://localhost', port=8000, auto_connect=True):
        """
        Creates a new instance of the TruenoDB client driver that connects to a remote database at the specified
        host and port. Automatic connection can be disabled to supply callback functions through connect method.
        Disconnection is automatic when this class instance is destroyed, but can also be manually triggered.
        :param host: Hostname of the Trueno database instance to connect to.
        :param port: Port number for connection.
        :param auto_connect: Automatically connect to the database after this class is instantiated.
        """
        self.__logger = logging.getLogger(__name__)

        self.host = host
        self.port = port

        # Connect
        self.rpc = RPC(self.host, self.port)

        if auto_connect:
            self.rpc.connect(None, None)

    def connect(self, connect_callback=None, disconnect_callback=None):
        """
        Explicitly connect to the Trueno database and register optional connect and disconnect callbacks.
        :param connect_callback: Function to be executed after connection is successful.
        :param disconnect_callback: Function to be executed after disconnecting from the Trueno database.
        """
        self.rpc.connect(connect_callback, disconnect_callback)

    def disconnect(self):
        """
        Explicitly disconnect from the Trueno database.
        """
        self.rpc.disconnect()

    def connected(self):
        """
        Verifies connection status with the remote Trueno Database.
        :return: True if connected, False if disconnected.
        """
        return self.rpc.connected()

    def graph(self, label):
        """
        Load a Graph from the remote Trueno Database.
        :param label: Graph label identifier
        :return: Trueno Graph reference
        """

        if label is None:
            self.__logger.warn('Graph label is required')
            return

        g = Graph()
        g.connection = self.rpc
        g.label = label

        return g

    def sql(self, query):
        """
        Submit a SQL query to the remote Trueno Database
        :param query: Query to be executed
        :return: Promise with result of query
        """
        api_fun = 'ex_sql'

        msg = Message()
        msg.payload['q'] = query

        self.__logger.debug(api_fun + ' – ' + json.dumps(msg.__dict__))

        return self.rpc.call(api_fun, msg)
