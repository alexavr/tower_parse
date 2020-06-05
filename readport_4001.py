#!/usr/bin/env python3.5

import re
import signal
import socket
import time

import logging
import logging.handlers

from datetime import datetime
from multiprocessing import Process, Queue, Event
from pathlib import Path
from queue import Empty

import numpy as np


STATION_NAME = "MSU"
SONIC_NAME = "Test1"
HOST, PORT = "192.168.192.48", 4001

FILL_VALUE = -999.0
PACK_LIMIT = 12000  # 12000 = 20*60*10 (10')

BUFSIZE = 1024  # Socket receiver buffer size
CHECKPOINT = 0  # Number of seconds between updates to the console (use 0 to disable)
LOG_LEVEL = "INFO"  # Use "DEBUG" to see more messages in the console and the log-files

# A flag that signals the processes to shut down
shutdown_event = Event()


class NoDataException(Exception):
    """A custom exception thrown when an empty message is received"""


class IncompleteDataException(Exception):
    """A custom exception signifying an incomplete message received"""


def interrupt_handler(sig, frame):
    """A handler for the Ctrl-C event."""
    logging.info("Exiting gracefully...")
    shutdown_event.set()


def configure_logging():
    """Setup logging to the file and the console
    """
    root = logging.getLogger()
    root.setLevel(LOG_LEVEL)

    log_filename = Path(__file__).with_suffix(".log").name

    # Setup a rotating log file. At most 5 backup copies are kept, less than 10 MB each.
    handler = logging.handlers.RotatingFileHandler(
        log_filename, mode="a", maxBytes=1e7, backupCount=5
    )
    formatter = logging.Formatter("%(asctime)s [%(levelname)s]: %(message)s")
    handler.setFormatter(formatter)
    root.addHandler(handler)

    # Setup logging to the console
    console = logging.StreamHandler()
    formatter = logging.Formatter("%(message)s")
    console.setFormatter(formatter)
    root.addHandler(console)


class Checkpoint:
    """Print the number of messages received every "interval" seconds.
    """

    def __init__(self, interval):
        """Initialize the class using the current system time and
        a checkpoint interval in seconds.

        Args:
            interval: print to the console every "interval" seconds.
                Use 0 to disable the checkpointing functionality.
        """
        self.interval = interval
        self.n_messages = 0
        self.start_time = time.time()

    def update(self, end_time):
        """Increment the number of messages received and print to the console
        if "interval" seconds have elapsed.

        Args:
            end_time: Current unix timestamp
        """
        if self.interval == 0:
            # Updates are disabled
            return

        self.n_messages += 1
        elapsed = end_time - self.start_time

        if elapsed >= self.interval:
            logging.info(
                "Received {:,} messages in {:.1f} seconds".format(
                    self.n_messages, elapsed
                )
            )
            self.n_messages = 0
            self.start_time = time.time()


def connect():
    """Establish socket connection, retrying if necessary
    """
    reconnecting = False
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    while not shutdown_event.is_set():
        try:
            sock.connect((HOST, PORT))
            logging.info(
                "Connected to {}:{}. Receiving device data...".format(HOST, PORT)
            )
            break
        except Exception:
            if not reconnecting:
                logging.info("Attempting to connect to socket...")
                reconnecting = True
            time.sleep(1)

    # Obtain a file hand capable or reading line by line
    f = sock.makefile(mode="rb")
    return sock, f


def listen_device(queue):
    """Receive messages from the device over a TCP socket and queue them
    for parallel processing.
    """
    # Connect to the device socket
    sock, f = connect()

    def cleanup():
        """Close the socket-associated handles."""
        try:
            f.close()
            sock.close()
        except Exception:
            logging.warning("File handle or socket already closed")

    # Initialize message counting and periodical updates to the console
    checkpoint = Checkpoint(interval=CHECKPOINT)

    while not shutdown_event.is_set():
        try:
            # Read complete messages ending in "\n"
            data = f.readline()
            if not data:
                raise NoDataException("Empty data received")
        except (OSError, NoDataException) as e:
            logging.warning(e)
            logging.info("Reconnecting...")
            cleanup()
            sock, f = connect()
            checkpoint = Checkpoint(interval=CHECKPOINT)
            continue

        # Get the current time for the received message. In a rare event that multiple
        # messages have been received over the socket at once, the timestamps for
        # individual messages will be very close to each other, but not the same.
        timestamp = time.time()

        # Send the received data and the timestamp to the second process for parsing
        queue.put([data, timestamp], timeout=1)

        # Print the number of messages received every CHECKPOINT seconds
        checkpoint.update(timestamp)

    cleanup()


def process_data(queue):
    """Take messages from the queue, parse them and periodically save to disk.
    """
    # Make sure the target directory exists
    p = Path("./data")
    p.mkdir(parents=True, exist_ok=True)

    # Initialize the temporary storage for parsed data
    data_list = []
    count = 0

    # Loop until a shutdown flag is set and all items in the queue have been received
    while not (shutdown_event.is_set() and queue.empty()):
        try:
            data, timestamp = queue.get(timeout=1)
        except Empty:
            # If the queue is empty, wait for messages that might arrive in the future
            continue

        try:
            u, v, w, t = parse(data)
            # Details below will be printed only when LOG_LEVEL="DEBUG"
            logging.debug(
                "Got u={:+06.2f}, v={:+06.2f}, w={:+06.2f}, t={:+06.2f}".format(
                    u, v, w, t
                )
            )
        except IncompleteDataException:
            # Completely skip incomplete messages (e.g. the very first message received
            # upon the start of the script)
            continue

        # Collect the parsed data
        data_list.append([timestamp, u, v, w, t])
        count += 1

        # Save the data to disk when the packing limit is reached
        if count == PACK_LIMIT:
            # Convert each variable to a separate NumPy vector
            timestamp, u, v, w, t = np.array(data_list).T

            # Save to a compressed file with a current timestamp (up to seconds)
            filename = p / "{station_name}_{sonic_name}_{timestr}.npz".format(
                station_name=STATION_NAME,
                sonic_name=SONIC_NAME,
                timestr=datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S"),
            )
            np.savez_compressed(filename, time=timestamp, u=u, v=v, w=w, temp=t)
            logging.info("Data saved to '{}'".format(filename))

            # Reset the in-memory storage
            data_list = []
            count = 0


def parse(data):
    """Extract the variabls u, v, w, t from the binary message.

    Ensures the message is complete, i.e. starts with ".Q" and ends with "\r\n",
    otherwise raises an exception. If unable to extract variables from a complete
    message, uses fill values for u, v, w, t and logs the event.

    Args:
        data: a binary message received from the device

    Returns:
        u, v, w, t: extracted float values or FILL_VALUES

    Raises:
        IncompleteDataException: if an unrecoverable incomplete message is received.
            In thise case, it doesn't make sense to use fill values for u, v, w, t.
    """
    # Test whether a complete message has been received
    pattern = rb"^\x02Q,.*,\x03..\r\n$"
    match = re.match(pattern, data)
    if not match:
        logging.warning("Incomplete message received, skipping: {}".format(data))
        raise IncompleteDataException("Incomplete message received")

    # Extract the floating point values from message
    pattern = rb"^.+,([^,]+),([^,]+),([^,]+),.,([^,]+),([^,]+),([^,]+)$"
    try:
        match = re.match(pattern, data)
        u, v, w, t = [float(value) for value in match.group(1, 2, 3, 4)]
    except (ValueError, AttributeError):
        # If the regex pattern produced no match or there was a type conversion error
        logging.error("Cannot parse message, substituting fill values: {}".format(data))
        u, v, w, t = [FILL_VALUE] * 4

    return u, v, w, t


def main():
    # Set up logging to the console and the log-files
    configure_logging()

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

