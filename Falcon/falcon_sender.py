import zmq
import os
import time
import uuid
import socket
import warnings
import datetime
import glob
import numpy as np
import logging as log
import multiprocessing as mp
from threading import Thread, enumerate as te
from concurrent.futures import ThreadPoolExecutor
from config_sender import configurations
from search import base_optimizer, dummy, brute_force, hill_climb, cg_opt, lbfgs_opt, gradient_opt_fast


def tcp_stats():
    """
    Collect TCP statistics for the given receiver address.

    Returns:
    Tuple containing the number of data segments sent (sent) and the number of retransmitted bytes (retm).
    """
    global RCVR_ADDR
    start = time.time()
    sent, retm = 0, 0

    try:
        data = os.popen("ss -ti").read().split("\n")
        for i in range(1, len(data)):
            if RCVR_ADDR in data[i - 1]:
                parse_data = data[i].split(" ")
                for entry in parse_data:
                    if "data_segs_out" in entry:
                        sent += int(entry.split(":")[-1])

                    if "bytes_retrans" in entry:
                        pass

                    elif "retrans" in entry:
                        retm += int(entry.split("/")[-1])

    except Exception as e:
        log.debug(e)

    end = time.time()
    log.debug("Time taken to collect tcp stats: {0}ms".format(np.round((end - start) * 1000)))
    return sent, retm


def worker(process_id, q):
    """
    Worker function for handling file transfer tasks in a separate process.

    This function represents an individual worker process responsible for transferring
    files based on the tasks received from the queue (`q`). It utilizes socket communication
    to send file chunks to the receiver.

    Parameters:
        process_id (int): Identifier for the worker process.
        q (Queue): Queue containing file transfer tasks.

    Returns:
        None
    """
    while file_incomplete.value > 0:
        # Check if the worker process is active
        if process_status[process_id] == 0:
            pass
        else:
            # Wait for at least one worker to be available
            while num_workers.value < 1:
                pass

            log.debug("Start Process :: {0}".format(process_id))

            # Emulab test parameters
            if emulab_test:
                target, factor = 20, 10
                max_speed = (target * 1000 * 1000) / 8
                second_target, second_data_count = int(max_speed / factor), 0

            # Process tasks from the queue
            while not q.empty() and process_status[process_id] == 1:
                try:
                    # Get the next file transfer task from the queue
                    file_id = q.get()
                except:
                    # If an error occurs, set the process status to 0 and break
                    process_status[process_id] = 0
                    break

                try:
                    # Create a socket and connect to the receiver
                    sock = socket.socket()
                    sock.settimeout(3)
                    sock.connect((HOST, PORT))

                    file_id, file_name, offset, to_send, dir_id= prepare_file_info(file_id)

                    # Check if there is data to send and the process is still active
                    if (to_send > 0) and (process_status[process_id] == 1):
                        file = open(file_name, "rb")
                        if dir_id is not None:
                            msg = file_name.split('/')[-1] + "," + str(int(offset[dir_id]))
                        else:
                            msg = file_name.split('/')[-1] + "," + str(int(offset))
                        msg += "," + str(int(to_send)) + "\n"
                        sock.send(msg.encode())

                        log.debug("starting {0}, {1}, {2}".format(process_id, file_id, file_name))
                        timer100ms = time.time()

                        while (to_send > 0) and (process_status[process_id] == 1):
                            # Emulab test: Send artificial data in fixed-size blocks
                            if emulab_test:
                                block_size = min(chunk_size, second_target - second_data_count)
                                data_to_send = bytearray(int(block_size))
                                sent = sock.send(data_to_send)
                            else:
                                # Normal case: Send file data using sendfile or fixed-size blocks
                                block_size = min(chunk_size, to_send)
                                if file_transfer:
                                    if dir_id is not None:
                                        sent = sock.sendfile(file=file, offset=int(offset[dir_id]), count=int(block_size))
                                    else:
                                        sent = sock.sendfile(file=file, offset=int(offset), count=int(block_size))
                                else:
                                    data_to_send = bytearray(int(block_size))
                                    sent = sock.send(data_to_send)

                            # Update offset and remaining data to send
                            to_send -= sent
                            if dir_id is not None:
                                offset[dir_id] += sent
                                file_offsets[file_id[0]] = offset
                            else:
                                offset += sent
                                file_offsets[file_id] = offset

                            # Emulab test: Sleep to simulate network delay
                            if emulab_test:
                                second_data_count += sent
                                if second_data_count >= second_target:
                                    second_data_count = 0
                                    while timer100ms + (1 / factor) > time.time():
                                        pass

                                    timer100ms = time.time()

                    handle_completion(file_id, file_name, dir_id, to_send)

                    # Close the socket
                    sock.close()

                except socket.timeout as e:
                    # Ignore socket timeout exceptions
                    pass

                except Exception as e:
                    # Set the process status to 0 and log any unexpected errors
                    process_status[process_id] = 0
                    log.debug("Process: {0}, Error: {1}".format(process_id, str(e)))

            log.debug("End Process :: {0}".format(process_id))

    # Set the process status to 0 when the loop exits
    process_status[process_id] = 0

def prepare_file_info(file_id):
    """
    Prepare file-related information based on the type of file_id.

    Parameters:
    - file_id: Identifier for the file to be transferred.

    Returns:
    - Tuple containing file-related information (file_id, file_name, offset, to_send, dir_id).
    """
    if type(file_id) is int:
        file_name = '/' + file_names[file_id].split('/', 1)[1]
        offset = file_offsets[file_id]
        to_send = file_sizes[file_id] - offset
        dir_id = None
    else:
        path = file_names[file_id[0]]
        dir_id = file_id[1].get()
        file_list = [file for file in glob.glob(path + '**', recursive=True) if os.path.isfile(file)]
        file_name = file_list[dir_id]
        offset = file_offsets[file_id[0]]
        to_send = file_sizes[file_id[0]][dir_id] - offset[dir_id]

    return file_id, file_name, offset, to_send, dir_id


def handle_completion(file_id, file_name, dir_id ,to_send):
    """
    Handle completion of the file transfer based on the remaining data to send.

    Parameters:
    - file_id: Identifier for the file to be transferred.
    - file_name: Name of the file to be transferred.
    - dir_id: Directory identifier for recursive file transfers.
    - to_send: Remaining bytes to be sent.

    Returns:
    None
    """
    # Check if there is still data to send and requeue the task if needed
    if to_send > 0:
        if dir_id is not None:
            file_id[1].put(dir_id)
        q.put(file_id)
    else:
        # Remove the file from the list or queue based on completion
        if dir_id is not None:
            file_id[2].remove(file_name)
            if not file_id[2]:
                finished_files.put(file_names[file_id[0]])
                with file_incomplete.get_lock():
                    file_incomplete.value -= 1
            else:
                q.put(file_id)
        else:
            # Mark the file as finished and update file_incomplete value
            finished_files.put(file_name)
            with file_incomplete.get_lock():
                file_incomplete.value -= 1


def event_receiver():
    """
    Receive events from the centralized coordinator.

    This function continuously listens for events from the centralized coordinator
    and adjusts the number of workers based on the received events.

    Returns:
        None
    """
    while file_incomplete.value > 0:
        try:
            # Receive events from the centralized coordinator
            resp = r_conn.xread({receive_key: 0}, count=None)

            # Process received events, if any
            if resp:
                key, messages = resp[0]
                for message in messages:
                    _, data = message
                    cc = int(data[b"cc"].decode("utf-8"))
                    r_conn.delete(key)

                    # Ensure cc is at least 1 and round to the nearest integer
                    cc = 1 if cc < 1 else int(np.round(cc))
                    num_workers.value = cc
                    log.info("Sample Transfer -- Probing Parameters: {0}".format(num_workers.value))

                    # Determine the current active communication channels (CC)
                    current_cc = np.sum(process_status)

                    # Adjust process status to activate or deactivate communication channels
                    for i in range(configurations["thread_limit"]):
                        if i < cc:
                            if (i >= current_cc):
                                process_status[i] = 1
                        else:
                            process_status[i] = 0

                    log.debug("Active CC: {0}".format(np.sum(process_status)))

        except Exception as e:
            # Log any exceptions that may occur during event reception
            log.exception(e)



def event_sender(sc, rc):
    """
    Send events to the centralized coordinator based on network statistics.

    This function calculates a score based on throughput, loss rate, and impacts.
    It then sends the calculated score as an event to the centralized coordinator.

    Parameters:
        sc (int): Number of TCP segments sent.
        rc (int): Number of TCP segments retransmitted.

    Returns:
        None
    """
    B, K = int(configurations["B"]), float(configurations["K"])
    score = exit_signal

    # Check if there are incomplete files for calculating the score
    if file_incomplete.value > 0:
        thrpt = np.mean(throughput_logs[-2:]) if len(throughput_logs) > 1 else 0
        lr = rc / sc if (sc > rc and sc != 0) else 0

        plr_impact = B * lr
        cc_impact_nl = K ** num_workers.value
        score = (thrpt / cc_impact_nl) - (thrpt * plr_impact)
        score = np.round(score * (-1))

    try:
        # Prepare data for the event (score) and send it to the centralized coordinator
        data = {"score": int(score)}
        r_conn.xadd(send_key, data)
    except Exception as e:
        # Log any exceptions that may occur during the event sending process
        log.exception(e)



def run_centralized():
    """
    Execute the centralized file transfer process.

    This function starts an event receiver thread and continuously monitors
    network statistics to send events to the centralized coordinator.

    Returns:
        None
    """
    # Start a separate thread for receiving events from workers
    receiver_thread = Thread(target=event_receiver)
    receiver_thread.start()

    # Initialize previous TCP statistics for comparison
    prev_sc, prev_rc = tcp_stats()

    # Continue monitoring network statistics and sending events
    while file_incomplete.value > 0:
        # Sleep for a short duration to avoid frequent monitoring
        time.sleep(0.9)

        # Measure current TCP statistics
        curr_sc, curr_rc = tcp_stats()

        # Calculate the change in TCP segments (Send Count and Retransmit Count)
        sc, rc = curr_sc - prev_sc, curr_rc - prev_rc
        prev_sc, prev_rc = curr_sc, curr_rc

        # Submit the event to the centralized coordinator
        executor.submit(event_sender, sc, rc)


def sample_transfer(params):
    """
    Execute a sample file transfer for probing optimization parameters.

    This function simulates a file transfer for probing optimization parameters.
    It adjusts the number of workers based on the provided parameters, measures
    network statistics, calculates a score, and logs relevant information.

    Parameters:
        params (list): List of parameters, where params[0] represents the number of workers.

    Returns:
        int: A score value reflecting the effectiveness of the current configuration.
             If file_incomplete.value becomes zero, it returns the exit_signal.
    """
    global throughput_logs, exit_signal

    # Check if there are no incomplete files
    if file_incomplete.value == 0:
        return exit_signal

    # Ensure params[0] is at least 1 and round to the nearest integer
    params = [1 if x < 1 else int(np.round(x)) for x in params]
    log.info("Sample Transfer -- Probing Parameters: {0}".format(params))

    # Update the number of workers based on the parameters
    num_workers.value = params[0]

    # Determine the current active communication channels (CC)
    current_cc = np.sum(process_status)

    # Adjust process status to activate or deactivate communication channels
    for i in range(configurations["thread_limit"]):
        if i < params[0]:
            if (i >= current_cc):
                process_status[i] = 1
        else:
            process_status[i] = 0

    log.debug("Active CC: {0}".format(np.sum(process_status)))

    # Wait for a short period to gather network statistics
    time.sleep(1)

    # Measure TCP statistics before the probing time window
    prev_sc, prev_rc = tcp_stats()

    # Set the end time of the probing time window
    n_time = time.time() + probing_time - 1.1

    # Collect network statistics during the probing time window
    while (time.time() < n_time) and (file_incomplete.value > 0):
        time.sleep(0.1)

    # Measure TCP statistics after the probing time window
    curr_sc, curr_rc = tcp_stats()

    # Calculate the change in TCP segments (Send Count and Retransmit Count)
    sc, rc = curr_sc - prev_sc, curr_rc - prev_rc

    log.debug("TCP Segments >> Send Count: {0}, Retrans Count: {1}".format(sc, rc))

    # Calculate loss rate (lr), packet loss rate impact (plr_impact),
    # and concurrency impact non-linear (cc_impact_nl)
    thrpt = np.mean(throughput_logs[-2:]) if len(throughput_logs) > 2 else 0
    lr, B, K = 0, int(configurations["B"]), float(configurations["K"])

    if sc != 0:
        lr = rc / sc if sc > rc else 0

    plr_impact = B * lr
    cc_impact_nl = K ** num_workers.value

    # Calculate the optimization score based on throughput, loss rate, and impacts
    score = (thrpt / cc_impact_nl) - (thrpt * plr_impact)
    score_value = np.round(score * (-1))

    # Log relevant information about the sample transfer
    log.info("Sample Transfer -- Throughput: {0}Mbps, Loss Rate: {1}%, Score: {2}".format(
        np.round(thrpt), np.round(lr * 100, 2), score_value))

    # Check if there are no incomplete files
    if file_incomplete.value == 0:
        return exit_signal
    else:
        return score_value


def normal_transfer(params):
    """
    Execute the normal file transfer process using the specified number of workers.

    This function initializes the number of workers based on the provided parameters
    and starts the normal file transfer process. It updates the process status until
    all workers have completed their tasks and there are still incomplete files.

    Parameters:
        params (list): List of parameters, where params[0] represents the number of workers.

    Returns:
        None
    """
    num_workers.value = max(1, int(np.round(params[0])))
    log.info("Normal Transfer -- Probing Parameters: {0}".format([num_workers.value]))

    # Set the process status for each worker to indicate the start of the transfer
    for i in range(num_workers.value):
        process_status[i] = 1

    # Continue the transfer process until all workers have completed their tasks
    # and there are still incomplete files
    while (np.sum(process_status) > 0) and (file_incomplete.value > 0):
        pass


def run_transfer():
    """
    Execute the file transfer process based on the specified optimization method.

    This function determines the optimization method based on the configuration and
    runs the corresponding file transfer process. If centralized mode is enabled, it
    runs the centralized transfer. Otherwise, it chooses from various optimization
    methods such as random, brute force, hill climb, gradient, conjugate, LBFGS, probing,
    or Bayesian optimization.

    Returns:
        None
    """
    params = [2]

    # Check if centralized mode is enabled
    if centralized:
        run_centralized()

    # Execute optimization based on the specified method in the configurations
    elif configurations["method"].lower() == "random":
        log.info("Running Random Optimization .... ")
        params = dummy(configurations, sample_transfer, log)

    elif configurations["method"].lower() == "brute":
        log.info("Running Brute Force Optimization .... ")
        params = brute_force(configurations, sample_transfer, log)

    elif configurations["method"].lower() == "hill_climb":
        log.info("Running Hill Climb Optimization .... ")
        params = hill_climb(configurations, sample_transfer, log)

    elif configurations["method"].lower() == "gradient":
        log.info("Running Gradient Optimization .... ")
        params = gradient_opt_fast(configurations, sample_transfer, log)

    elif configurations["method"].lower() == "cg":
        log.info("Running Conjugate Optimization .... ")
        params = cg_opt(configurations, sample_transfer)

    elif configurations["method"].lower() == "lbfgs":
        log.info("Running LBFGS Optimization .... ")
        params = lbfgs_opt(configurations, sample_transfer)

    elif configurations["method"].lower() == "probe":
        log.info("Running a fixed configurations Probing .... ")
        params = [configurations["fixed_probing"]["thread"]]

    else:
        log.info("Running Bayesian Optimization .... ")
        params = base_optimizer(configurations, sample_transfer, log)

    # If there are incomplete files, execute the normal transfer process
    if file_incomplete.value > 0:
        normal_transfer(params)


def report_throughput(start_time):
    """
    Monitor and report throughput during the file transfer process.

    This function continuously calculates and logs throughput information at regular intervals.

    Parameters:
        start_time (float): The start time of the file transfer process.

    Returns:
        None
    """
    global throughput_logs
    previous_total = 0
    previous_time = 0

    while file_incomplete.value > 0:
        t1 = time.time()
        time_since_begining = np.round(t1 - start_time, 1)

        if time_since_begining >= 0.1:
            if time_since_begining >= 3 and sum(throughput_logs[-3:]) == 0:
                file_incomplete.value = 0

            # Calculate total bytes transferred and current throughput
            total_bytes = sum(item if isinstance(item, float) else sum(item) for item in file_offsets)
            thrpt = np.round((total_bytes * 8) / (time_since_begining * 1000 * 1000), 2)

            # Calculate current throughput over the last interval
            curr_total = total_bytes - previous_total
            curr_time_sec = np.round(time_since_begining - previous_time, 3)
            curr_thrpt = np.round((curr_total * 8) / (curr_time_sec * 1000 * 1000), 2)
            previous_time, previous_total = time_since_begining, total_bytes

            # Log throughput information
            throughput_logs.append(curr_thrpt)
            m_avg = np.round(np.mean(throughput_logs[-60:]), 2)

            log.info("Throughput @{0}s: Current: {1}Mbps, Average: {2}Mbps, 60Sec_Average: {3}Mbps".format(
                time_since_begining, curr_thrpt, thrpt, m_avg))

            t2 = time.time()
            # Sleep to maintain a regular reporting interval
            time.sleep(max(0, 1 - (t2 - t1)))


def main():
    """
    The main function orchestrating the file transfer process.

    This function initializes necessary variables, starts worker processes,
    initiates the reporting process, runs the file transfer, and calculates
    and logs the total transfer time and throughput.

    Returns:
        None
    """
    global file_incomplete, process_status, file_offsets

    # Reset values for file completeness, process status, and file offsets
    file_incomplete = mp.Value("i", file_count)
    process_status = mp.Array("i", [0 for i in range(configurations["thread_limit"])])

    if centralized:
        try:
            # Register transfer ID in Redis (if centralized mode is enabled)
            r_conn.xadd(register_key, {"transfer_id": transfer_id})
        except ConnectionError as e:
            log.error(f"Redis Connection Error: {e}")

    global q

    # Create worker processes for concurrent file transfer
    workers = [mp.Process(target=worker, args=(i, q)) for i in range(configurations["thread_limit"])]

    for p in workers:
        p.daemon = True
        p.start()

    # Start reporting process for throughput monitoring
    start = time.time()
    reporting_process = mp.Process(target=report_throughput, args=(start,))
    reporting_process.daemon = True
    reporting_process.start()

    # Run the file transfer process
    run_transfer()

    # End time measurement
    end = time.time()

    # Calculate and log total transfer time and throughput
    time_since_begining = np.round(end - start, 3)
    total = np.round(sum(item if isinstance(item, float) else sum(item) for item in file_offsets) / (1024 * 1024 * 1024), 3)
    thrpt = np.round((total * 8 * 1024) / time_since_begining, 2)
    log.info("Total: {0} GB, Time: {1} sec, Throughput: {2} Mbps".format(
        total, time_since_begining, thrpt))

    # Terminate reporting process and worker processes
    reporting_process.terminate()

    for p in workers:
        if p.is_alive():
            p.terminate()
            p.join(timeout=0.1)


def update_arguments(filepath):
    """
    Update the arguments for file processing based on the provided filepath.

    This function updates global variables to track file information, sizes, and offsets.
    It also populates queues with file-related information for further processing.

    Parameters:
        filepath (str): The path to the file or directory to be processed.

    Returns:
        None
    """
    global file_count, file_incomplete, file_offsets
    global q

    # Append the filepath to the list of file names
    file_names.append(filepath)

    if os.path.isdir(filepath):
        # If the provided path is a directory, process all files recursively
        all_files = glob.glob(filepath + '**', recursive=True)

        # Filter out directories to get a list of files
        dir_files = [file for file in all_files if os.path.isfile(file)]
        dir_files_list = manager.list(dir_files)

        # Get file sizes for all files in the directory
        file_sizes.append([os.path.getsize(filename) for filename in dir_files])

        # Initialize file offsets for each file in the directory
        file_offsets.append([0.0] * len(dir_files))

        # Create a queue for indexing files in the directory
        dir_queue = manager.Queue(-1)
        for i in range(len(os.listdir(filepath))):
            dir_queue.put(i)

        # Populate the main queue with information about files in the directory
        for _ in range(len(os.listdir(filepath))):
            q.put((file_count, dir_queue, dir_files_list))
    else:
        # If the provided path is a file, get its size and set initial offset
        file_sizes.append(os.path.getsize('/' + filepath.split('/', 1)[1]))
        file_offsets.append(0.0)
        q.put(file_count)

    # Increment the count of incomplete files
    with file_incomplete.get_lock():
        file_incomplete.value += 1

    # Increment the total file count
    file_count += 1


def parsl_acknowledge():
    """
    A function to acknowledge and process messages related to finished files using ZeroMQ.

    This function runs in an infinite loop, waiting for acknowledgment messages from clients,
    processing the messages, and sending appropriate replies back to the client.

    Logs:
        Exception: If an error occurs during the acknowledgment processing, except when the
                   ZeroMQ socket or context is closed.

    Returns:
        None
    """
    finished_files_list = []
    while True:
        try:
            # Wait for acknowledgment message from client
            message = zmq_socket_ack.recv_string()

            # Check if there are finished files in the queue and update the list
            if not finished_files.empty():
                finished_files_list.extend([finished_files.get() for _ in range(finished_files.qsize())])

            # Process acknowledgment message
            if message not in finished_files_list:
                # File not found in the list, send "False" reply
                zmq_socket_ack.send_string("False")
            else:
                # Remove the file from the list and send acknowledgment reply
                finished_files_list.remove(message)
                log.debug(f'Ack for file {message}', message)
                zmq_socket_ack.send_string("Acknowledgment done for %s " % message)
        except zmq.error.ContextTerminated:
            # Context was terminated, exit the loop
            break
        except Exception as e:
            # An unexpected exception was encountered and logged
            log.debug(e)
            break


def parsl_receiver():
    """
    A function to receive and process messages from clients using ZeroMQ.

    This function runs in an infinite loop, waiting for incoming requests from clients,
    processing the messages, and sending a reply back to the client.

    Logs:
        Exception: If an error occurs during the acknowledgment processing, except when the
                   ZeroMQ socket or context is closed.

    Returns:
        None
    """
    while True:
        try:
            #  Wait for next request from client
            message = zmq_socket_receiver.recv_string()
            log.debug("Received request: %s" % message)

            # Process the received message
            update_arguments(message)

            #  Send reply back to client
            zmq_socket_receiver.send_string("Queue update with " + message)
        except zmq.error.ContextTerminated:
            # Context was terminated, exit the loop
            break
        except Exception as e:
            # An unexpected exception was encountered and logged
            log.debug(e)
            break

# Disable FutureWarnings
warnings.filterwarnings("ignore", category=FutureWarning)

# Set CPU count and thread limit
configurations["cpu_count"] = mp.cpu_count()
configurations["thread_limit"] = configurations["max_cc"]

# Set thread limit if not specified
if configurations["thread_limit"] == -1:
    configurations["thread_limit"] = configurations["cpu_count"]

# Configure logging
log_FORMAT = '%(created)f -- %(levelname)s: %(message)s'
log_file = "logs/" + datetime.datetime.now().strftime("%m_%d_%Y_%H_%M_%S") + ".log"

# Set log level based on configurations
if configurations["loglevel"] == "debug":
    log.basicConfig(
        format=log_FORMAT,
        datefmt='%m/%d/%Y %I:%M:%S %p',
        level=log.DEBUG,
    )

    mp.log_to_stderr(log.DEBUG)
elif configurations["loglevel"] == "info":
    log.basicConfig(
        format=log_FORMAT,
        datefmt='%m/%d/%Y %I:%M:%S %p',
        level=log.INFO,
    )

# Set Emulab test and centralized flags
emulab_test = configurations.get("emulab_test", False)
centralized = configurations.get("centralized", False)

# Initialize ThreadPoolExecutor
executor = ThreadPoolExecutor(max_workers=5)

# Connect to Redis if centralized
if centralized:
    from redis import Redis

    transfer_id = str(uuid.uuid4())
    hostname = os.environ.get("REDIS_HOSTNAME", "localhost")
    port = os.environ.get("REDIS_PORT", 6379)
    send_key = f"report:{transfer_id}"
    receive_key = f"cc:{transfer_id}"
    register_key = f"transfer-registration"
    r_conn = Redis(hostname, port, retry_on_timeout=True)

# Set file transfer flag
file_transfer = configurations.get("file_transfer", True)

# Set probing time
probing_time = configurations["probing_sec"]

# Initialize multiprocessing manager and queues
manager = mp.Manager()
finished_files = manager.Queue(-1)
throughput_logs = manager.list()
q = manager.Queue(-1)

file_names = manager.list()
file_sizes = manager.list()
file_offsets = manager.list()
file_count = 0

# Set exit signal, chunk size, num_workers, file_incomplete, and process_status
exit_signal = 10 ** 10
chunk_size = 1 * 1024 * 1024
num_workers = mp.Value("i", 0)
file_incomplete = mp.Value("i", file_count)
process_status = mp.Array("i", [0 for i in range(configurations["thread_limit"])])

# Set host, port, and receiver address
HOST, PORT = configurations["receiver"]["host"], configurations["receiver"]["port"]
RCVR_ADDR = str(HOST) + ":" + str(PORT)

# Initialize ZeroMQ context and sockets
zmq_context = zmq.Context()
zmq_socket_receiver = zmq_context.socket(zmq.REP)
zmq_socket_receiver.bind("tcp://*:5555")
zmq_socket_ack = zmq_context.socket(zmq.REP)
zmq_socket_ack.bind("tcp://*:5556")

# Start updater and acknowledger threads
parsl_receiver = Thread(target=parsl_receiver, args=())
parsl_receiver.start()
parsl_acknowledge = Thread(target=parsl_acknowledge, args=())
parsl_acknowledge.start()

# Main loop
while True:
    if file_count > 0:
        print("Running main() ... ")
        main()
        print("Finished main() ... ")
        time.sleep(1)
        zmq_socket_receiver.close()
        zmq_socket_ack.close()
        zmq_context.term()
        break
    else:
        time.sleep(1)
