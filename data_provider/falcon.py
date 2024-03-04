import logging
import time
from functools import partial

import zmq
import sqlite3
from threading import Thread
from parsl.app.app import python_app
from parsl.data_provider.staging import Staging
from parsl.utils import RepresentationMixin

# Initialize the logger
logger = logging.getLogger(__name__)


def _get_falcon_provider(dfk, executor_label):
    """
    Internal function that retrieves the appropriate Falcon provider for a given executor label.

    Args:
        dfk (DataFlowKernel): A Parsl DataFlowKernel object.
        executor_label (str): The label of the executor for which to retrieve a Falcon provider.

    Returns:
        FalconStaging: A FalconStaging object that provides access to the Falcon transfer service.
    """
    if executor_label is None:
        raise ValueError("executor_label is mandatory")
    executor = dfk.executors[executor_label]
    if not hasattr(executor, "storage_access"):
        raise ValueError("specified executor does not have storage_access attribute")
    for provider in executor.storage_access:
        if isinstance(provider, FalconStaging):
            return provider

    raise Exception('No suitable Falcon endpoint defined for executor {}'.format(executor_label))


def get_falcon():
    """
    Function that initializes a Falcon object.

    Returns:
        Falcon: A Falcon object that provides access to the Falcon transfer service.
    """
    Falcon.init()
    return Falcon()


class Falcon(object):
    """
    Class that defines methods for transferring files using the Falcon transfer service.
    """

    @classmethod
    def init(cls):
        """
        Class method that initializes the Falcon object.
        """
        pass

    @classmethod
    def transfer_file(cls, path, netloc):
        """
        Class method that transfers a file to a specified netloc using the ZeroMQ messaging library.

        Args:
            path (str): The path to the file to transfer.
            netloc (str): The network location to which the file should be transferred from.
        """
        zmq_context = zmq.Context()

        # Initialize a REQ socket and connect to the specified netloc
        zmq_socket = zmq_context.socket(zmq.REQ)
        zmq_socket.connect("tcp://" + netloc + ":5555")

        # Send the path of the file to transfer
        zmq_socket.send_string(path)

        # Receive a response from the server
        zmq_socket.recv_string()

        # Initialize another REQ socket and connect to the specified netloc
        zmq_socket1 = zmq_context.socket(zmq.REQ)
        zmq_socket1.connect("tcp://" + netloc + ":5556")

        # Continuously send the path of the file to transfer until the server confirms that the transfer is complete
        while True:
            zmq_socket1.send_string(path)
            message1 = zmq_socket1.recv_string()
            if message1 == "False":
                time.sleep(1)
            else:
                logger.info(message1)
                break


class FalconStaging(Staging, RepresentationMixin):

    def can_stage_in(self, file):
        """
        Returns True if the input file can be staged in, False otherwise.
        """
        logger.debug("Falcon checking file {}".format(repr(file)))
        return file.scheme == 'falcon'

    def can_stage_out(self, file):
        """
        Returns True if the output file can be staged out, False otherwise.
        """
        logger.debug("Falcon checking file {}".format(repr(file)))
        return file.scheme == 'falcon'

    def stage_in(self, dm, executor, file, parent_fut):
        """
        Stages in a file using Falcon.

        Parameters:
        - dm: DataMover instance
        - executor: the executor to be used
        - file: the file to be staged in
        - parent_fut: the future representing the parent task

        Returns:
        - the future representing the staged in file
        """
        falcon_provider = _get_falcon_provider(dm.dfk, executor)
        # falcon_provider_update_local_path(file, executor, dm.dfk)
        stage_in_app = falcon_provider._falcon_stage_in_app(executor=executor, dfk=dm.dfk)
        app_fut = stage_in_app(outputs=[file], _parsl_staging_inhibit=True, parent_fut=parent_fut)
        return app_fut._outputs[0]

    def stage_out(self, dm, executor, file, app_fu):
        """
        Stages out a file using Falcon.

        Parameters:
        - dm: DataMover instance
        - executor: the executor to be used
        - file: the file to be staged out
        - app_fu: the future representing the application that produced the file

        Returns:
        - the future representing the staged out file
        """
        falcon_provider = _get_falcon_provider(dm.dfk, executor)
        # falcon_provider_update_local_path(file, executor, dm.dfk)
        stage_out_app = falcon_provider._falcon_stage_out_app(executor=executor, dfk=dm.dfk)
        return stage_out_app(app_fu, _parsl_staging_inhibit=True, inputs=[file])

    def __init__(self):
        """
        Initializes a FalconStaging instance.
        """
        self.falcon = None

    def _falcon_stage_in_app(self, executor, dfk):
        """
        Returns a Parsl app that stages in a file using Falcon.

        Parameters:
        - executor: the executor to be used
        - dfk: the data flow kernel

        Returns:
        - a Parsl app that stages in a file using Falcon
        """
        executor_obj = dfk.executors[executor]
        f = partial(_falcon_stage_in, self, executor_obj)
        return python_app(executors=['_parsl_internal'], data_flow_kernel=dfk)(f)

    def _falcon_stage_out_app(self, executor, dfk):
        """
        Returns a Parsl app that stages out a file using Falcon.

        Parameters:
        - executor: the executor to be used
        - dfk: the data flow kernel

        Returns:
        - a Parsl app that stages out a file using Falcon
        """
        executor_obj = dfk.executors[executor]
        f = partial(_falcon_stage_out, self, executor_obj)
        return python_app(executors=['_parsl_internal'], data_flow_kernel=dfk)(f)

    def initialize_falcon(self):
        """
        Initializes Falcon if it hasn't been initialized already.
        """
        if self.falcon is None:
            self.falcon = get_falcon()


    def _update_local_path(self, file, executor, dfk):
        """
        Updates the local path of the given file.

        Args:
            file (File): The file to be updated.
            executor (str): The name of the executor.
            dfk (DataFlowKernel): The DataFlowKernel object.
        """
        executor_obj = dfk.executors[executor]
        # file.local_path = os.path.join(globus_ep['working_dir'], file.filename)


def _falcon_stage_in(provider, executor, parent_fut=None, outputs=[], _parsl_staging_inhibit=True):
    # The input file is the first output file.
    file = outputs[0]

    # Ensure the Falcon instance has been initialized.
    provider.initialize_falcon()

    # Transfer the file from the source location to the Falcon cluster.
    provider.falcon.transfer_file(file.path, file.netloc)


def _falcon_stage_out(provider, executor, app_fu, inputs=[], _parsl_staging_inhibit=True):
    # The output file is the first input file.
    file = inputs[0]

    # Ensure the Falcon instance has been initialized.
    provider.initialize_falcon()

    # Transfer the file from the source location to the Falcon cluster.
    provider.falcon.transfer_file(file.path, file.netloc)
