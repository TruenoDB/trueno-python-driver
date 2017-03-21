from socketIO_client import SocketIO, BaseNamespace
from promise import Promise
from urllib import parse
import logging
import logging.config
from enum import Enum

__author__ = 'Miguel Rivera'
__version__ = '0.1.0'


class Status(Enum):
    """
    Status Enumerator Class
    """
    Success = 'success'
    Error = 'error'


class RPC:
    """
    Provides the communication facilities to interact with a TruenoDB instance
    """

    def __init__(self, host="http://localhost", port=8000, procedures=None):
        """
        Initialization of Host and Port for TruenoDB connection, hostname validation and database connection.
        :param host: TruenoDB Host to connect to
        :param port: Port to use for TruenoDB connection
        :param procedures: List of procedures to execute in remote TruenoDB database
        """
        self.__logger = logging.getLogger(__name__)

        self.__host = host
        self.__port = port

        if procedures is None:
            self.__procedures = []
        else:
            self.__procedures = procedures

        verify_url = parse.urlparse(self.__host)
        if not getattr(verify_url, 'scheme') or not getattr(verify_url, 'netloc'):
            self.__logger.warning('Provided connection URL is invalid, aborting!')
            return

        self.__promise = Promise
        self.__socket = None

    def connect(self, conn_callback, disc_callback):
        """
        Connect to the remote database and register optional provided callbacks.
        :param conn_callback: function to execute when a connection to the remote database is established.
        :param disc_callback: function to execute when a connection to the remote database is closed.
        """
        b = BaseNamespace
        if conn_callback is not None:
            b.on_connect = conn_callback
        if disc_callback is not None:
            b.on_disconnect = disc_callback
        self.__socket = SocketIO(self.__host, self.__port, b, wait_for_connection=False)

    def disconnect(self):
        """
        Close the connection to the remote database.
        """
        if self.__socket is not None:
            self.__socket.disconnect()

    def connected(self):
        """
        Check if there is an active connection to the database.
        :return: True if connected, False otherwise.
        """
        if self.__socket is None:
            return False
        else:
            return self.__socket.connected

    def call(self, method, arg):
        """
        Execute a request to the remote TruenoDB database.
        :param method: Remote method to execute
        :param arg: Arguments provided to the remote call
        :return: Promise with result from async operation.
        """
        if self.__socket is not None:
            self.__socket.emit(method, arg, self.on_ack)

            return self.__promise
        else:
            self.__logger.warn('Not connected to a Trueno database instance.')

            return

    def on_ack(self, *args):
        """
        Handles the response of the TruenoDB server on a method call
        :param args: Arguments returned by the TruenoDB server
        :return: Promise with result from async operation
        """
        self.__logger.debug(*args)

        response = args[0]
        if getattr(args, 'status'):
            if getattr(args, 'status') == Status.Success:
                self.__promise = Promise.resolve(response)
            if getattr(args, 'status') == Status.Error:
                self.__promise = Promise.rejected(response)
