#!/usr/bin/env python3.5

import argparse
import configparser
import logging
import logging.handlers
import re
import signal
import socket
import time

from datetime import datetime
from multiprocessing import Process, Queue, Event
from pathlib import Path
from queue import Empty

import numpy as np

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


class Checkpoint:
    """Print the number of messages received every "interval" seconds.
    """

    def __init__(self, interval):
        """Initialize the class using the current system time and
        a checkpoint interval in seconds.

        Args:
            interval: print to the console every "interval" seconds.
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


def checkpoint_factory(interval):
    """Create an instance of the Checkpoint class. If 0 interval is used, avoid all
    computations altogether.

    Args:
        interval: print to the console every "interval" seconds.
            Use 0 to disable the checkpointing functionality.

    Returns:
        checkpoint: an instantiated Checkpoint object
    """
    checkpoint = Checkpoint(interval)

    if interval == 0:
        # Perform no computations
        checkpoint.update = lambda x: None

    return checkpoint


def connect(host, port):
    """Establish socket connection, retrying if necessary

    Args:
        host: IP address of the device
        port: port number to listen to

    Returns:
        sock, f: a socket handler and an associated file handler for reading line by line
    """
    reconnecting = False
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    while not shutdown_event.is_set():
        try:
            if not reconnecting:
                logging.info(
                    "Attempting to connect to socket at {}:{}...".format(host, port)
                )
                reconnecting = True
            sock.connect((host, port))
            logging.info("Connected. Receiving device data...".format(host, port))
            break
        except Exception:
            time.sleep(1)

    # Obtain a file descriptor capable or reading line by line
    f = sock.makefile(mode="rb")
    return sock, f


def listen_device(queue, host, port, checkpoint_interval):
    """Receive messages from the device over a TCP socket and queue them
    for parallel processing.

    Args:
        queue: a multiprocessing queue to send data to
        host: IP address of the device
        port: port number to listen to
        checkpoint_interval: seconds between updates to the console
    """
    # Connect to the device socket
    sock, f = connect(host, port)

    def cleanup():
        """Close the socket-associated handles."""
        try:
            f.close()
            sock.close()
        except Exception:
            # The socket may already be closed
            pass

    # Initialize message counting and periodical updates to the console
    checkpoint = checkpoint_factory(checkpoint_interval)

    while not shutdown_event.is_set():
        try:
            # Read complete messages ending in "\n". If a partial message is received,
            # buffer and wait for the remainder before continuing. If multiple joined
            # messages are obtained, split them into individual records.
            data = f.readline()
            if not data:
                raise NoDataException("Empty data received")
        except (OSError, NoDataException) as e:
            logging.warning(e)
            logging.info("Reconnecting")
            cleanup()
            sock, f = connect(host, port)
            checkpoint = checkpoint_factory(checkpoint_interval)
            continue

        # Get the current time for the received message. In a rare event that multiple
        # messages have been received over the socket at once, the timestamps for
        # individual messages will be very close to each other, but not the same.
        timestamp = time.time()

        # Send the received data and the timestamp to the second process for parsing
        queue.put([data, timestamp], timeout=1)

        # Print the number of messages received every checkpoint_interval seconds
        checkpoint.update(timestamp)

    cleanup()


def process_data(queue, fill_value, pack_limit, station_name, sonic_name):
    """Take messages from the queue, parse them and periodically save to disk.

    Args:
        queue: a multiprocessing queue to read messages from
        fill_value: a number used instead of real values when parsing fails
        pack_limit: the total number of records to store on disk at once
        station_name: a string identificator of the meteo station
        sonic_name: a string identificator of the device
    """
    # Make sure the target directory exists
    p = Path("./data")
    p.mkdir(parents=True, exist_ok=True)

    # Initialize the temporary storage for parsed data
    data_list = []

    # Loop until a shutdown flag is set and all items in the queue have been received
    while not (shutdown_event.is_set() and queue.empty()):
        try:
            data, timestamp = queue.get(timeout=1)
        except Empty:
            # If the queue is empty, wait for messages that might arrive in the future
            continue

        try:
            u, v, w, t = parse(data, fill_value)
            # Details below will be printed only when log_level="DEBUG"
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

        # Save the data to disk when the packing limit is reached
        if len(data_list) == pack_limit:
            # Convert each variable to a separate NumPy vector
            timestamp, u, v, w, t = np.array(data_list).T

            # Save to a compressed file with a current timestamp (up to seconds)
            filename = p / "{station_name}_{sonic_name}_{timestr}.npz".format(
                station_name=station_name,
                sonic_name=sonic_name,
                timestr=datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S"),
            )
            np.savez_compressed(filename, time=timestamp, u=u, v=v, w=w, temp=t)
            logging.info("Data saved to '{}'".format(filename))

            # Reset the in-memory storage
            data_list = []


def parse(data, fill_value):
    """Extract the variables u, v, w, t from the binary message.

    Ensures the message is complete, i.e. starts with ".Q" and ends with "\r\n",
    otherwise raises an exception. If unable to extract variables from a complete
    message, uses fill values for u, v, w, t and logs the event.

    Args:
        data: a binary message received from the device
        fill_value: a number used instead of real values when parsing fails

    Returns:
        u, v, w, t: extracted float values or fill_values

    Raises:
        IncompleteDataException: when an unrecoverable incomplete message is received.
            In this case, it doesn't make sense to use fill values for u, v, w, t.
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
        u, v, w, t = [fill_value] * 4

    return u, v, w, t


def read_cmdline():
    """Parse the command-line arguments.

    Returns:
        args: an object with the values of the command-line options
    """
    parser = argparse.ArgumentParser(description="Read and save sonic data.")
    # For better clarity, add a required block in the description
    required = parser.add_argument_group("required arguments")
    required.add_argument(
        "-c", "--config", help="path to the configuration file", required=True,
    )
    args = parser.parse_args()
    return args


def load_config(path):
    """Load the configuration file with correct parameter data types.

    Args:
        path: filename of the config file

    Returns:
        conf: a Namespace object with the loaded settings
    """
    # Interpolation is used e.g. for expanding the log file name
    config = configparser.ConfigParser(
        interpolation=configparser.ExtendedInterpolation()
    )
    config.read(path)

    # Flatten the structure and convert the types of the parameters
    conf = dict(
        station_name=config.get("device", "station_name"),
        sonic_name=config.get("device", "sonic_name"),
        host=config.get("device", "host"),
        port=config.getint("device", "port"),
        fill_value=config.getfloat("parser", "fill_value"),
        pack_limit=config.getint("parser", "pack_limit"),
        log_level=config.get("logging", "log_level"),
        log_file=config.get("logging", "log_file"),
        checkpoint=config.getint("logging", "checkpoint"),
    )

    # Convert the dictionary to a Namespace object, to enable .attribute access
    conf = argparse.Namespace(**conf)
    return conf


def configure_logging(log_level, log_file):
    """Setup rotated logging to the file and the console

    Args:
        log_level: the threshold for the logging system ("INFO", "DEBUG", etc.)
        log_file: the filename of the log to write to
    """
    root = logging.getLogger()
    root.setLevel(log_level)

    # Setup a rotating log file. At most 5 backup copies are kept, less than 10 MB each.
    handler = logging.handlers.RotatingFileHandler(
        log_file, mode="a", maxBytes=1e7, backupCount=5
    )
    formatter = logging.Formatter("%(asctime)s [%(levelname)s]: %(message)s")
    handler.setFormatter(formatter)
    root.addHandler(handler)

    # Setup simultaneous logging to the console
    console = logging.StreamHandler()
    formatter = logging.Formatter("%(message)s")
    console.setFormatter(formatter)
    root.addHandler(console)


def main():
    # Parse the command-line arguments and load the config file
    args = read_cmdline()
    conf = load_config(path=args.config)

    # Set up logging to the console and the log-files
    configure_logging(log_level=conf.log_level, log_file=conf.log_file)
    logging.info("Logging to the file '{}'".format(conf.log_file))

    # Ignore Ctrl-C in subprocesses
    signal.signal(signal.SIGINT, signal.SIG_IGN)

    # Create a communication queue between processes
    queue = Queue()

    # Launch the subprocesses
    p1 = Process(
        target=listen_device,
        kwargs=dict(
            queue=queue,
            host=conf.host,
            port=conf.port,
            checkpoint_interval=conf.checkpoint,
        ),
    )
    p2 = Process(
        target=process_data,
        kwargs=dict(
            queue=queue,
            fill_value=conf.fill_value,
            pack_limit=conf.pack_limit,
            station_name=conf.station_name,
            sonic_name=conf.sonic_name,
        ),
    )
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
