import zmq
import os
import time
import uuid
import socket
import warnings
import datetime
import numpy as np
import logging as log
import multiprocessing as mp
from threading import Thread, Lock
from concurrent.futures import ThreadPoolExecutor
from config_sender import configurations
from search import base_optimizer, dummy, brute_force, hill_climb, cg_opt, lbfgs_opt, gradient_opt_fast


def tcp_stats():
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
    while file_incomplete.value > 0:
        if process_status[process_id] == 0:
            pass
        else:
            while num_workers.value < 1:
                pass
            log.debug("Start Process :: {0}".format(process_id))
            # try:
            #     sock = socket.socket()
            #     sock.settimeout(3)
            #     sock.connect((HOST, PORT))

            if emulab_test:
                target, factor = 20, 10
                max_speed = (target * 1000 * 1000) / 8
                second_target, second_data_count = int(max_speed / factor), 0

            while (not q.empty()) and (process_status[process_id] == 1):
                try:
                    file_id = q.get()
                except:
                    process_status[process_id] = 0
                    break

                try:
                    # HOST = file_names[file_id].split('/', 1)[0]
                    sock = socket.socket()
                    sock.settimeout(3)
                    sock.connect((HOST, PORT))

                    if type(file_id) is int:
                        file_name = '/' + file_names[file_id].split('/', 1)[1]
                        offset = file_offsets[file_id]
                        to_send = file_sizes[file_id] - offset
                        if (to_send > 0) and (process_status[process_id] == 1):
                            file = open(file_name, "rb")
                            msg = file_name.split('/')[-1] + "," + str(int(offset))
                            msg += "," + str(int(to_send)) + "\n"
                            sock.send(msg.encode())

                            log.debug("starting {0}, {1}, {2}".format(process_id, file_id, file_name))
                            timer100ms = time.time()

                            while (to_send > 0) and (process_status[process_id] == 1):
                                if emulab_test:
                                    block_size = min(chunk_size, second_target - second_data_count)
                                    data_to_send = bytearray(int(block_size))
                                    sent = sock.send(data_to_send)
                                else:
                                    block_size = min(chunk_size, to_send)
                                    if file_transfer:
                                        sent = sock.sendfile(file=file, offset=int(offset), count=int(block_size))
                                        # data = os.preadv(file, block_size, offset)
                                    else:
                                        data_to_send = bytearray(int(block_size))
                                        sent = sock.send(data_to_send)

                                offset += sent
                                to_send -= sent
                                file_offsets[file_id] = offset

                                if emulab_test:
                                    second_data_count += sent
                                    if second_data_count >= second_target:
                                        second_data_count = 0
                                        while timer100ms + (1 / factor) > time.time():
                                            pass

                                        timer100ms = time.time()
                        if to_send > 0:
                            q.put(file_id)
                        else:
                            finished_files.put(file_name)
                            with file_incomplete.get_lock():
                                file_incomplete.value -= 1
                    else:
                        print('Processing Dir...')
                        path = file_names[file_id[0]]
                        dir_id = file_id[1]
                        print('dir_id', dir_id)
                        file_list = os.listdir(path)
                        file_name = path + file_list[dir_id]
                        print('file_name', file_name)
                        offset = file_offsets[file_id[0]]
                        to_send = [a - b for a, b in zip(file_sizes[file_id[0]], offset)]
                        if (to_send[dir_id] > 0) and (process_status[process_id] == 1):
                            file = open(file_name, "rb")
                            msg = file_name.split('/')[-1] + "," + str(int(offset[dir_id]))
                            msg += "," + str(int(to_send[dir_id])) + "\n"
                            print('msg: ',msg)
                            sock.send(msg.encode())

                            log.debug("starting {0}, {1}, {2}, {3}".format(process_id, file_id, dir_id, file_name))
                            timer100ms = time.time()

                            while (to_send[dir_id] > 0) and (process_status[process_id] == 1):
                                if emulab_test:
                                    block_size = min(chunk_size, second_target - second_data_count)
                                    data_to_send = bytearray(int(block_size))
                                    sent = sock.send(data_to_send)
                                else:
                                    block_size = min(chunk_size, to_send[dir_id])
                                    if file_transfer:
                                        sent = sock.sendfile(file=file, offset=int(offset[dir_id]), count=int(block_size))
                                        # data = os.preadv(file, block_size, offset)
                                    else:
                                        data_to_send = bytearray(int(block_size))
                                        sent = sock.send(data_to_send)

                                offset[dir_id] += sent
                                to_send[dir_id] -= sent
                                file_offsets[file_id[0]] = offset

                                if emulab_test:
                                    second_data_count += sent
                                    if second_data_count >= second_target:
                                        second_data_count = 0
                                        while timer100ms + (1 / factor) > time.time():
                                            pass

                                        timer100ms = time.time()
                        if to_send[dir_id] > 0:
                            file_id[1].put(dir_id)
                        else:
                            if not file_id[1].empty():
                                q.put(file_id)
                            else:
                                print('Here')
                                finished_files.put(file_names[file_id[0]])
                                with file_incomplete.get_lock():
                                    file_incomplete.value -= 1



                    sock.close()
                except socket.timeout as e:
                    pass

                except Exception as e:
                    process_status[process_id] = 0
                    log.debug("Process: {0}, Error: {1}".format(process_id, str(e)))

            log.debug("End Process :: {0}".format(process_id))

    process_status[process_id] = 0


def event_receiver():
    while file_incomplete.value > 0:
        try:
            resp = r_conn.xread({receive_key: 0}, count=None)
            if resp:
                key, messages = resp[0]
                for message in messages:
                    _, data = message
                    cc = int(data[b"cc"].decode("utf-8"))
                    r_conn.delete(key)

                    cc = 1 if cc < 1 else int(np.round(cc))
                    num_workers.value = cc
                    log.info("Sample Transfer -- Probing Parameters: {0}".format(num_workers.value))

                    current_cc = np.sum(process_status)
                    for i in range(configurations["thread_limit"]):
                        if i < cc:
                            if (i >= current_cc):
                                process_status[i] = 1
                        else:
                            process_status[i] = 0

                    log.debug("Active CC: {0}".format(np.sum(process_status)))
        except Exception as e:
            log.exception(e)


def event_sender(sc, rc):
    B, K = int(configurations["B"]), float(configurations["K"])
    score = exit_signal
    if file_incomplete.value > 0:
        thrpt = np.mean(throughput_logs[-2:]) if len(throughput_logs) > 1 else 0
        lr = rc / sc if (sc > rc and sc != 0) else 0

        plr_impact = B * lr
        cc_impact_nl = K ** num_workers.value
        score = (thrpt / cc_impact_nl) - (thrpt * plr_impact)
        score = np.round(score * (-1))

    try:
        data = {}
        data["score"] = int(score)
        r_conn.xadd(send_key, data)
    except Exception as e:
        log.exception(e)


def run_centralized():
    receiver_thread = Thread(target=event_receiver)
    receiver_thread.start()

    prev_sc, prev_rc = tcp_stats()
    while file_incomplete.value > 0:
        time.sleep(0.9)
        curr_sc, curr_rc = tcp_stats()
        sc, rc = curr_sc - prev_sc, curr_rc - prev_rc
        prev_sc, prev_rc = curr_sc, curr_rc
        executor.submit(event_sender, sc, rc)


def sample_transfer(params):
    global throughput_logs, exit_signal

    if file_incomplete.value == 0:
        return exit_signal

    params = [1 if x < 1 else int(np.round(x)) for x in params]
    log.info("Sample Transfer -- Probing Parameters: {0}".format(params))
    num_workers.value = params[0]

    current_cc = np.sum(process_status)
    for i in range(configurations["thread_limit"]):
        if i < params[0]:
            if (i >= current_cc):
                process_status[i] = 1
        else:
            process_status[i] = 0

    log.debug("Active CC: {0}".format(np.sum(process_status)))

    time.sleep(1)
    prev_sc, prev_rc = tcp_stats()
    n_time = time.time() + probing_time - 1.1
    while (time.time() < n_time) and (file_incomplete.value > 0):
        time.sleep(0.1)

    curr_sc, curr_rc = tcp_stats()
    sc, rc = curr_sc - prev_sc, curr_rc - prev_rc

    log.debug("TCP Segments >> Send Count: {0}, Retrans Count: {1}".format(sc, rc))
    thrpt = np.mean(throughput_logs[-2:]) if len(throughput_logs) > 2 else 0

    lr, B, K = 0, int(configurations["B"]), float(configurations["K"])
    if sc != 0:
        lr = rc / sc if sc > rc else 0

    plr_impact = B * lr
    cc_impact_nl = K ** num_workers.value
    score = (thrpt / cc_impact_nl) - (thrpt * plr_impact)
    score_value = np.round(score * (-1))

    log.info("Sample Transfer -- Throughput: {0}Mbps, Loss Rate: {1}%, Score: {2}".format(
        np.round(thrpt), np.round(lr * 100, 2), score_value))

    if file_incomplete.value == 0:
        return exit_signal
    else:
        return score_value


def normal_transfer(params):
    num_workers.value = max(1, int(np.round(params[0])))
    log.info("Normal Transfer -- Probing Parameters: {0}".format([num_workers.value]))

    for i in range(num_workers.value):
        process_status[i] = 1

    while (np.sum(process_status) > 0) and (file_incomplete.value > 0):
        pass


def run_transfer():
    params = [2]

    if centralized:
        run_centralized()

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

    if file_incomplete.value > 0:
        normal_transfer(params)


def report_throughput(start_time):
    global throughput_logs
    previous_total = 0
    previous_time = 0

    while file_incomplete.value > 0:
        t1 = time.time()
        time_since_begining = np.round(t1 - start_time, 1)

        if time_since_begining >= 0.1:
            if time_since_begining >= 3 and sum(throughput_logs[-3:]) == 0:
                file_incomplete.value = 0

            total_bytes = np.sum(file_offsets)
            thrpt = np.round((total_bytes * 8) / (time_since_begining * 1000 * 1000), 2)

            curr_total = total_bytes - previous_total
            curr_time_sec = np.round(time_since_begining - previous_time, 3)
            curr_thrpt = np.round((curr_total * 8) / (curr_time_sec * 1000 * 1000), 2)
            previous_time, previous_total = time_since_begining, total_bytes
            throughput_logs.append(curr_thrpt)
            m_avg = np.round(np.mean(throughput_logs[-60:]), 2)

            log.info("Throughput @{0}s: Current: {1}Mbps, Average: {2}Mbps, 60Sec_Average: {3}Mbps".format(
                time_since_begining, curr_thrpt, thrpt, m_avg))
            t2 = time.time()
            time.sleep(max(0, 1 - (t2 - t1)))


def main():
    global file_incomplete, process_status, file_offsets

    file_incomplete = mp.Value("i", file_count)
    process_status = mp.Array("i", [0 for i in range(configurations["thread_limit"])])

    if centralized:
        try:
            r_conn.xadd(register_key, {"transfer_id": transfer_id})
        except ConnectionError as e:
            log.error(f"Redis Connection Error: {e}")
    global q

    workers = [mp.Process(target=worker, args=(i, q)) for i in range(configurations["thread_limit"])]
    for p in workers:
        p.daemon = True
        p.start()

    start = time.time()
    reporting_process = mp.Process(target=report_throughput, args=(start,))
    reporting_process.daemon = True
    reporting_process.start()
    run_transfer()
    end = time.time()

    time_since_begining = np.round(end - start, 3)
    total = np.round(np.sum(file_offsets) / (1024 * 1024 * 1024), 3)
    thrpt = np.round((total * 8 * 1024) / time_since_begining, 2)
    log.info("Total: {0} GB, Time: {1} sec, Throughput: {2} Mbps".format(
        total, time_since_begining, thrpt))

    reporting_process.terminate()
    for p in workers:
        if p.is_alive():
            p.terminate()
            p.join(timeout=0.1)


def update_arguments(filepath):
    global file_count, file_incomplete, file_offsets
    global q
    file_names.append(filepath)
    if os.path.isdir(filepath):
        dir_files = os.listdir(filepath)
        file_sizes.append([os.path.getsize(filepath+filename) for filename in dir_files])

        file_offsets.append([0] * len(dir_files))
        dir_tuple=(file_count,manager.Queue(-1))
        for i in range(len(os.listdir(filepath))):
            # q.put((file_count,i))
            dir_tuple[1].put(i)
        q.put(dir_tuple)
    else:
        file_sizes.append(os.path.getsize('/' + filepath.split('/', 1)[1]))
        file_offsets.append(0.0)
        q.put(file_count)
    with file_incomplete.get_lock():
        file_incomplete.value += 1
    file_count += 1


def ack():
    finished_files_list = [finished_files.get() for _ in range(finished_files.qsize())]
    while True:
        try:
            message = zmq_socket2.recv_string()
            if not finished_files.empty():
                finished_files_list = finished_files_list + [finished_files.get() for _ in range(finished_files.qsize())]
            if message not in finished_files_list:
                zmq_socket2.send_string("False")
            else:
                finished_files_list.remove(message)
                log.debug(f'Ack for file {message}', message)
                zmq_socket2.send_string("Acknowledgment done for %s " % message)
        except Exception as e:
            log.debug(e)
            break


def thread_function():
    while True:
        try:
            #  Wait for next request from client
            message = zmq_socket.recv_string()
            log.debug("Received request: %s" % message)

            update_arguments(message)

            #  Send reply back to client
            zmq_socket.send_string("Queue update with " + message)
        except Exception as e:
            log.debug(e)
            break


if __name__ == '__main__':
    warnings.filterwarnings("ignore", category=FutureWarning)
    configurations["cpu_count"] = mp.cpu_count()
    configurations["thread_limit"] = configurations["max_cc"]

    if configurations["thread_limit"] == -1:
        configurations["thread_limit"] = configurations["cpu_count"]

    log_FORMAT = '%(created)f -- %(levelname)s: %(message)s'
    log_file = "logs/" + datetime.datetime.now().strftime("%m_%d_%Y_%H_%M_%S") + ".log"

    if configurations["loglevel"] == "debug":
        log.basicConfig(
            format=log_FORMAT,
            datefmt='%m/%d/%Y %I:%M:%S %p',
            level=log.DEBUG,
            handlers=[
                log.FileHandler(log_file),
                log.StreamHandler()
            ]
        )

        mp.log_to_stderr(log.DEBUG)
    elif configurations["loglevel"] == "info":
        log.basicConfig(
            format=log_FORMAT,
            datefmt='%m/%d/%Y %I:%M:%S %p',
            level=log.INFO,
            handlers=[
                log.FileHandler(log_file),
                log.StreamHandler()
            ]
        )

    emulab_test = False
    if "emulab_test" in configurations and configurations["emulab_test"] is not None:
        emulab_test = configurations["emulab_test"]

    centralized = False
    if "centralized" in configurations and configurations["centralized"] is not None:
        centralized = configurations["centralized"]

    executor = ThreadPoolExecutor(max_workers=5)
    if centralized:
        from redis import Redis

        transfer_id = str(uuid.uuid4())
        hostname = os.environ.get("REDIS_HOSTNAME", "localhost")
        port = os.environ.get("REDIS_PORT", 6379)
        send_key = f"report:{transfer_id}"
        receive_key = f"cc:{transfer_id}"
        register_key = f"transfer-registration"
        r_conn = Redis(hostname, port, retry_on_timeout=True)

    file_transfer = True
    if "file_transfer" in configurations and configurations["file_transfer"] is not None:
        file_transfer = configurations["file_transfer"]

    probing_time = configurations["probing_sec"]

    manager = mp.Manager()
    finished_files = manager.Queue(-1)
    throughput_logs = manager.list()
    q = manager.Queue(-1)

    file_names = manager.list()
    file_sizes = manager.list()
    file_offsets = manager.list()
    file_count = 0

    exit_signal = 10 ** 10
    chunk_size = 1 * 1024 * 1024
    num_workers = mp.Value("i", 0)
    file_incomplete = mp.Value("i", file_count)
    process_status = mp.Array("i", [0 for i in range(configurations["thread_limit"])])

    HOST, PORT = configurations["receiver"]["host"], configurations["receiver"]["port"]
    PORT = configurations["receiver"]["port"]
    RCVR_ADDR = str(HOST) + ":" + str(PORT)

    zmq_context = zmq.Context()
    zmq_socket = zmq_context.socket(zmq.REP)
    zmq_socket.bind("tcp://*:5555")
    zmq_socket2 = zmq_context.socket(zmq.REP)
    zmq_socket2.bind("tcp://*:5556")

    lock = Lock()
    updater = Thread(target=thread_function, args=())
    updater.start()
    acknowledger = Thread(target=ack, args=())
    acknowledger.start()
    start_time=time.time()
    while True:
        if file_count > 0:
            main()
            zmq_socket.close()
            zmq_socket2.close()
            zmq_context.term()
            break
        else:
            time.sleep(1)