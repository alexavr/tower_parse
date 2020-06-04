#!/usr/bin/env python3.5

import re
import signal
import socket
import time

from datetime import datetime
from multiprocessing import Process, Queue, Event
from pathlib import Path
from queue import Empty

import numpy as np


STATION_NAME = "MSU"
SONIC_NAME = "Test2"
HOST, PORT = "192.168.192.48", 4002

FILL_VALUE = -999.0
PACK_LIMIT = 12000  # 12000 = 20*60*10 (10')

BUFSIZE = 1024  # Receiver buffer size. Set to 1024 in production.
CHECKPOINT = 0  # Number of seconds between checkpoints (use 0 to disable)

# A flag that signals the processes to shut down
shutdown_event = Event()


class NoDataException(Exception):
    """A custom exception thrown when an empty message is received"""


def interrupt_handler(sig, frame):
    """A handler for the Ctrl-C event."""
    print("Exiting gracefully...")
    shutdown_event.set()


def connect():
    """Establish socket connection, retrying if necessary
    """
    reconnecting = False
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    while not shutdown_event.is_set():
        try:
            sock.connect((HOST, PORT))
            print("Connected to {}:{}. Receiving device data...".format(HOST, PORT))
            break
        except Exception:
            if not reconnecting:
                print("Attempting to connect to socket...")
            reconnecting = True
            time.sleep(1)

    return sock


def listen_device(queue):
    """Receive messages from the device over a TCP socket and queue them
    for parallel processing.
    """
    sock = connect()

    start = time.time()
    n_messages = 0

    while not shutdown_event.is_set():
        try:
            data = sock.recv(BUFSIZE)
            if not data:
                raise NoDataException("Empty data received")
        except (OSError, NoDataException) as e:
            print(e)
            print("Reconnecting...")
            try:
                sock.close()
            except Exception:
                # The socket might already be closed
                pass
            sock = connect()
            continue

        timestamp = time.time()

        queue.put([data, timestamp], timeout=1)

        n_messages += 1
        elapsed = timestamp - start

        if elapsed >= CHECKPOINT and CHECKPOINT > 0:
            print(
                "Received {:,} messages in {:.1f} seconds".format(n_messages, elapsed)
            )
            n_messages = 0
            start = time.time()

    sock.close()


def process_data(queue):
    """Take messages from the queue, parse them and periodically save to disk.
    """
    data_list = []
    count = 0

    # Make sure the target directory exists
    p = Path("./data")
    p.mkdir(parents=True, exist_ok=True)

    # Loop until a shutdown flag is set and all items in the queue have been received
    while not (shutdown_event.is_set() and queue.empty()):
        try:
            data, timestamp = queue.get(timeout=1)
        except Empty:
            # If the queue is empty, wait messages that might arrive in the future
            continue

        u, v, w, t = parse(data)
        # print("Got u={:05.2f}, v={:05.2f}, w={:05.2f}, t={:05.2f}".format(u, v, w, t))

        data_list.append([timestamp, u, v, w, t])
        count += 1

        # Save the data to disk
        if count == PACK_LIMIT:
            timestamp, u, v, w, t = np.array(data_list).T
            filename = p / "{station_name}_{sonic_name}_{timestr}.npz".format(
                station_name=STATION_NAME,
                sonic_name=SONIC_NAME,
                timestr=datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S"),
            )
            np.savez_compressed(filename, time=timestamp, u=u, v=v, w=w, temp=t)
            print("Data saved to '{}'".format(filename))

            data_list = []
            count = 0


def parse(data):
    pattern = r"^.+,([^,]+),([^,]+),([^,]+),.,([^,]+),([^,]+),([^,]+)$"

    try:
        match = re.match(pattern, data.decode())
        u, v, w, t = [float(value) for value in match.group(1, 2, 3, 4)]
    except (UnicodeDecodeError, ValueError, AttributeError):
        # If the regex pattern produced no match or there was a type conversion error
        u, v, w, t = [FILL_VALUE] * 4

    return u, v, w, t


def main():
    # Ignore Ctrl-C in subprocesses
    signal.signal(signal.SIGINT, signal.SIG_IGN)

    # Create a communication queue between processes
    queue = Queue()

    # Launch the subprocesses
    p1 = Process(target=listen_device, args=[queue])
    p2 = Process(target=process_data, args=[queue])
    p1.start()
    p2.start()

    # Gracefully handle Ctrl-C
    signal.signal(signal.SIGINT, interrupt_handler)

    # Wait for the subprocesses to complete
    p1.join()
    p2.join()
    queue.close()
    queue.join_thread()


if __name__ == "__main__":
    main()
