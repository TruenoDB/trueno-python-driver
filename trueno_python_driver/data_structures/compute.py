from .component import Component, ComponentType
from ..communication.message import Message
from enum import Enum
from promise import Promise
import logging
import json

__author__ = 'Miguel Rivera'
__version__ = '0.1.0'


class Algorithm(Enum):
    """
    Algorithm Enumerator Class
    """
    Dependencies = 1
    Page_Rank = 2
    Word_Count = 3
    Triangle_Counting = 4
    Connected_Components = 5
    Strongly_Connected_Components = 6
    Shortest_Paths = 7
    Undefined = 8


class JobStatus(Enum):
    """
    Job Status Enumerator Class
    """
    STARTED = 1
    FINISHED = 2
    RUNNING = 3
    ERROR = 4


class Compute(Component):
    """
    Represents a job to be executed on the TruenoDB Compute Server.
    """
    def __init__(self):
        """
        Creates a new Compute instance.
        """
        super(self.__class__, self).__init__()

        self.__logger = logging.getLogger(__name__)

        self.type = ComponentType.Compute
        self.jobId = None
        self.algorithm = Algorithm.Undefined
        self.parameters = {}

    def deploy(self):
        """
        Deploy the job in the Spark Cluster.
        :return: Promise with job ID.
        """
        api_fun = 'ex_compute'

        if not self.validate_graph_label():
            self.__logger.warning('Graph label is empty – ID: ' + self.id)
            return Promise.rejected('Graph label is empty – ID: ' + self.id)

        if self.algorithm.Undefined:
            self.__logger.warning('Algorithm type is Undefined')
            return Promise.rejected('Algorithm type is Undefined')

        msg = Message()
        msg.payload['graph'] = self.parent_graph.label
        msg.payload['algorithmType'] = self.algorithm
        msg.payload['subgraph'] = 'schema'
        msg.payload['parameters'] = self.parameters

        self.__logger.debug(api_fun + ' – ' + json.dumps(msg.__dict__))

    def job_status(self, job_id):
        """
        Request Status of deployed job.
        :param job_id: Job ID whose status will be queried.
        :return: Promise with status of the job.
        """
        api_fun = 'ex_computeJobStatus'

        if not self.validate_graph_label():
            self.__logger.warning('Graph label is empty – ID: ' + self.id)
            return Promise.rejected('Graph label is empty – ID: ' + self.id)

        msg = Message()
        msg.payload['jobId'] = job_id

        self.__logger.debug(api_fun + ' – ' + json.dumps(msg.__dict__))

    def job_result(self, job_id):
        """
        Request the result of deployed job.
        :param job_id: Job ID to retrieve result.
        :return: Promise with the result of the requested job.
        """
        api_fun = 'ex_computeJobResult'

        if not self.validate_graph_label():
            self.__logger.warning('Graph label is empty – ID: ' + self.id)
            return Promise.rejected('Graph label is empty – ID: ' + self.id)

        msg = Message()
        msg.payload['jobId'] = job_id

        self.__logger.debug(api_fun + ' – ' + json.dumps(msg.__dict__))

    def get_algorithms(self):
        """
        Get the list of algorithms supported by the server.
        :return: Promise with list of algorithms.
        """
        api_fun = 'ex_computeJobResult'

        if not self.validate_graph_label():
            self.__logger.warning('Graph label is empty – ID: ' + self.id)
            return Promise.rejected('Graph label is empty – ID: ' + self.id)

        msg = Message()
        msg.payload['getAlgorithms'] = True

        self.__logger.debug(api_fun + ' – ' + json.dumps(msg.__dict__))
