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


def falcon_feedback(cwd, netloc ='127.0.0.1', update_after = 5, margin_accepted = 0.1):
    return Thread(target=_falcon_feedback, args=(cwd, netloc, update_after, margin_accepted))


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


def _falcon_feedback(cwd, netloc, update_after, margin_accepted):
    # Connect to the SQLite database
    conn = sqlite3.connect(cwd + '/runinfo/monitoring.db')
    cursor = conn.cursor()

    retry_count = 0
    max_retries = 5
    retry_delay = 3  # Initial retry delay in seconds

    specified_time_str = '1970-01-01 00:00:00'

    # Set up ZeroMQ context and socket
    zmq_context = zmq.Context()
    zmq_socket = zmq_context.socket(zmq.REQ)
    zmq_socket.connect("tcp://" + netloc + ":5557")

    # Set a timeout for the zmq channel
    zmq_socket.setsockopt(zmq.RCVTIMEO, 10000)  # Timeout of 10 second
    time.sleep(2)

    while retry_count < max_retries:
        try:
            # Execute the first SELECT query to fetch the last run_id
            cursor.execute("SELECT run_id FROM task ORDER BY task_time_invoked DESC LIMIT 1")
            last_row = cursor.fetchone()
            run_id = last_row[0]
            cursor.execute("""
                        SELECT COUNT(*) 
                            FROM task 
                            WHERE run_id = ?
                            AND task_depends IS NOT ''""", (run_id,))
            total_tasks = cursor.fetchone()[0]

            # Set to store IDs of processed files
            processed_files = set()

            done_counter = 0
            total_proc_time = 0
            total_transfer_time = 0

            if total_tasks < update_after:
                update_after = total_tasks

            while True:
                # Execute the second SELECT query with the run_id as a parameter
                cursor.execute("""
                    SELECT t1.task_id,
                           (julianday(t1.task_time_returned) - julianday(t2.task_time_returned)) * 24 * 60 * 60 AS dependent_task_time,
                           (julianday(t2.task_time_returned) - julianday(t2.task_time_invoked)) * 24 * 60 * 60 AS transfer_time
                    FROM task AS t1
                    INNER JOIN task AS t2 ON t1.task_depends = t2.task_id
                    WHERE t1.task_depends IS NOT ''
                          AND t1.task_time_returned IS NOT ''
                          AND t1.run_id = ?
                          AND t2.run_id = ?
                          AND t1.task_id NOT IN ({})
                          AND t1.task_time_invoked > ?
                    ORDER BY t1.task_time_invoked DESC;
                """.format(','.join(['?'] * len(processed_files))),
                               (run_id, run_id) + tuple(processed_files) + (specified_time_str,))

                # Fetch all rows from the result set
                rows = cursor.fetchall()

                for row in rows:
                    task_id, processing_time, transfer_time = row
                    if processing_time is not None:
                        # Mark task as processed
                        total_proc_time += processing_time
                        total_transfer_time += transfer_time
                        processed_files.add(task_id)
                        print(processed_files)
                        done_counter += 1
                if done_counter >= update_after:
                    total_tasks -= done_counter
                    speed_up_factor = total_transfer_time / total_proc_time
                    if abs(speed_up_factor - 1) < margin_accepted:
                        logger.info("Speed is suitable")
                    else:
                        logger.info(f'A adjustment of %s is needed' % speed_up_factor)

                        # Send speedup factor over ZeroMQ channel
                        try:
                            print("About to send speed_up_factor of ", speed_up_factor )
                            zmq_socket.send_string(str(speed_up_factor))

                            # Receive a response from the server
                            specified_time_str = zmq_socket.recv_string()
                            cursor.execute("""
                                                    SELECT COUNT(*) 
                                                        FROM task 
                                                        WHERE run_id = ?
                                                        AND task_depends IS NOT ''
                                                        AND task_time_invoked > ?""", (run_id, specified_time_str,))
                            total_tasks = cursor.fetchone()[0]
                        except zmq.Again:
                            logger.debug("Zmq operation timed out")
                            print("Zmq operation timed out")
                            break
                    done_counter = 0
                    total_proc_time = 0
                    total_transfer_time = 0

                if total_tasks < update_after:
                    print("speedup done")
                    break
                else:
                    # Wait before checking again
                    time.sleep(retry_delay)

            # Break out of the retry loop if fetch was successful
            break

        except sqlite3.Error as e:
            if "database is locked" in str(e):
                # Retry after a brief delay
                logger.debug("Retrying fetch after database lock...")
                print("Retrying fetch after database lock...")
                retry_count += 1
                time.sleep(retry_delay)
                retry_delay *= 2
            else:
                # If the error is not "database is locked", raise it
                logger.error("SQLite error:", e)
                break
    # Close the cursor and the connection
    cursor.close()
    conn.close()
    print("Thread is done")
