import json

__author__ = 'Miguel Rivera'
__version__ = '0.1.0'


class Message:
    """
    Message class. Defines structures needed to interact with the Trueno database.
    """

    def __init__(self):
        """
        Initializes structures required in a Trueno Message.
        """
        self.meta = {}
        self.payload = {}
        self.type = {}
        self.status = {}


class BasePayload:
    """
    Payloads class. Used to initialize required fields in a payload to emit a Trueno Message successfully.
    """

    def __init__(self, graph):
        self.graph = graph


class CreatePayload(BasePayload):
    def __init__(self, graph):
        super(self.__class__, self).__init__(graph)
