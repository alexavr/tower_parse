#!/usr/bin/env python3.5

import argparse
import configparser
import logging
import logging.handlers
import re
import signal
import socket
import sys
import time

from ast import literal_eval
from collections import OrderedDict
from datetime import datetime
from multiprocessing import Process, Queue, Event
from pathlib import Path
from queue import Empty

import numpy as np

# A flag that signals the processes to shut down
shutdown_event = Event()

# A list of subprocesses
processes = []


def signal_handler(sig, frame):
    """A handler for the Ctrl-C event and the TERM signal."""
    if shutdown_event.is_set() or sig == signal.SIGTERM:
        # Terminate immediately
        logging.info("Terminating")
        for p in processes:
            p.terminate()
        sys.exit(1)
    else:
        # Set the shutdown flag
        logging.info(
            "Exiting gracefully... Press Ctrl-C again to terminate immediately."
        )
        shutdown_event.set()


class NoDataException(Exception):
    """A custom exception thrown when an empty message is received"""


class ParseError(Exception):
    """A custom exception signifying a parsing error"""


class Checkpoint:
    """Print the number of messages received every "interval" seconds.
    """

    def __init__(self, interval):
        """Initialize the class using the current system time and
        a checkpoint interval in seconds.

        Args:
            interval: print to the console every "interval" seconds.
                 Use 0 to disable printing.
        """
        self.interval = interval
        self.n_messages = 0
        self.fresh_connection = True
        self.start_time = time.time()

    def update(self, end_time):
        """Increment the number of messages received and print to the console
        if "interval" seconds have elapsed.

        Args:
            end_time: Current unix timestamp
        """
        self.fresh_connection = False

        if self.interval == 0:
            # Do nothing
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


def connect(host, port, timeout=None):
    """Establish socket connection, retrying if necessary

    Args:
        host: IP address of the device
        port: port number to listen to
        timeout: a timeout in seconds for connecting and reading data (default: None)

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
            sock.settimeout(timeout)
            sock.connect((host, port))
            logging.info(
                "Connected to {}:{}. Receiving device data...".format(host, port)
            )
            break
        except Exception:
            time.sleep(1)

    # Obtain a file descriptor capable or reading line by line
    f = sock.makefile(mode="rb")
    return sock, f


def listen_device(queue, conf):
    """Receive messages from the device over a TCP socket and queue them
    for parallel processing.

    Args:
        queue: a multiprocessing queue to send data to
        conf: a configuration Namespace object
    """
    # Connect to the device socket
    sock, f = connect(conf.host, conf.port, conf.timeout)

    def cleanup():
        """Close the socket-associated handles."""
        try:
            f.close()
            sock.close()
        except Exception:
            # The socket may already be closed
            pass

    # Initialize message counting and periodical updates to the console
    checkpoint = Checkpoint(conf.checkpoint_interval)

    while not shutdown_event.is_set():
        try:
            # Read complete messages ending in "\n". If a partial message is received,
            # buffer and wait for the remainder before continuing. If multiple joined
            # messages are obtained, split them into individual records.
            data = f.readline()
            if not data:
                raise NoDataException("Empty data received")
        except (OSError, NoDataException) as e:
            if isinstance(e, NoDataException):
                logging.warning(e)
            else:
                if isinstance(e, socket.timeout):
                    e = "Read timed out. No messages received in {} seconds.".format(
                        conf.timeout
                    )
                logging.error(e)

            if shutdown_event.is_set():
                continue
            logging.info("Reconnecting")
            cleanup()
            sock, f = connect(conf.host, conf.port, conf.timeout)
            checkpoint = Checkpoint(conf.checkpoint_interval)
            continue

        # Get the current time for the received message. In a rare event that multiple
        # messages have been received over the socket at once, the timestamps for
        # individual messages will be very close to each other, but not the same.
        timestamp = time.time()

        # Send the received data and the timestamp to the second process for parsing
        queue.put([data, timestamp, checkpoint.fresh_connection], timeout=1)

        # Print the number of messages received every checkpoint_interval seconds
        checkpoint.update(timestamp)

    cleanup()


def process_data(queue, conf):
    """Take messages from the queue, parse them and periodically save to disk.

    Args:
        queue: a multiprocessing queue to read messages from
        conf: a configuration Namespace object
    """
    # Make sure the target directory exists
    p = Path("./data")
    p.mkdir(parents=True, exist_ok=True)

    # Initialize the temporary storage for parsed data
    data_list = []

    # Loop until a shutdown flag is set and all items in the queue have been received
    while not (shutdown_event.is_set() and queue.empty()):
        try:
            data, timestamp, fresh_connection = queue.get(timeout=1)
        except Empty:
            # If the queue is empty, wait for messages that might arrive in the future
            continue

        try:
            variables = parse(data, conf)
            # Details below will be printed only when log_level="DEBUG"
            logging.debug("Got {}".format(dict(variables)))
        except ParseError:
            # The regex pattern produced no match or there was a type conversion error.
            if fresh_connection:
                # We expect the very first message received upon establishing
                # a connection to be often incomplete.
                logging.debug("Possibly incomplete first message: {}".format(data))
            else:
                logging.error("Cannot parse message: {}".format(data))
            continue

        # Collect the parsed data, saving only the values
        variables["time"] = timestamp
        data_list.append(tuple(variables.values()))

        # Save the data to disk when the packing limit is reached
        if len(data_list) == conf.pack_limit:
            # Convert each variable to a separate NumPy vector
            vectors = OrderedDict(zip(variables.keys(), np.array(data_list).T))

            # Save to a compressed file with a current timestamp (up to seconds)
            filename = p / "{station_name}_{device_name}_{timestr}.npz".format(
                station_name=conf.station_name,
                device_name=conf.device_name,
                timestr=datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S"),
            )
            np.savez_compressed(filename, **vectors)
            logging.info("Data saved to '{}'".format(filename))

            # Reset the in-memory storage
            data_list = []


def parse(data, conf):
    """Extract the variables from the binary message.

    Args:
        data: a binary message received from the device
        conf: a configuration Namespace object

    Returns:
        variables: a dictionary of extracted float values or fill_values

    Raises:
        ParseError: unable to parse a message due to issues with regex matching
        (including a possible incomplete message received) or failed conversion to float.
    """
    # Extract the values from the message
    try:
        match = re.match(conf.regex, data)
        values = [float(value) * conf.multiplier for value in match.groups()]
    except (ValueError, AttributeError):
        raise ParseError

    variables = OrderedDict(zip(conf.var_names, values))

    return variables


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
    try:
        with open(path) as f:
            config.read_file(f)
    except Exception as e:
        logging.error(e)
        sys.exit(1)

    def read_bytes(section, option):
        """Read an option from the config file as bytes"""
        value = config.get(section, option, raw=True)
        return literal_eval("b'{}'".format(value))

    # Flatten the structure and convert the types of the parameters
    conf = dict(
        station_name=config.get("device", "station_name"),
        device_name=config.get("device", "device_name"),
        host=config.get("device", "host"),
        port=config.getint("device", "port"),
        regex=read_bytes("parser", "regex"),
        var_names=config.get("parser", "var_names").split(),
        multiplier=config.getfloat("parser", "multiplier"),
        pack_limit=config.getint("parser", "pack_limit"),
        timeout=config.getint("parser", "timeout", fallback=None),
        log_level=config.get("logging", "log_level"),
        log_file=config.get("logging", "log_file"),
        checkpoint_interval=config.getint("logging", "checkpoint_interval"),
    )

    # Convert the dictionary to a Namespace object, to enable .attribute access
    conf = argparse.Namespace(**conf)

    # Ensure that "time" isn't used as a var_name in the config file
    if "time" in conf.var_names:
        logging.error(
            "Don't use 'time' among var_names in the config file. "
            "It is reserved for the message timestamp."
        )
        sys.exit(1)

    # Handle timeout=0 as None, which sets the socket in blocking mode without timeouts
    if conf.timeout == 0:
        conf.timeout = None

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
    p1 = Process(target=listen_device, kwargs=dict(queue=queue, conf=conf))
    p2 = Process(target=process_data, kwargs=dict(queue=queue, conf=conf))
    global processes
    processes = [p1, p2]
    p1.start()
    p2.start()

    # Gracefully handle Ctrl-C and the TERM signal
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Wait for the subprocesses to complete
    p1.join()
    p2.join()
    queue.close()
    queue.join_thread()


if __name__ == "__main__":
    main()
