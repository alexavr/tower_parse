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
from collections import defaultdict, namedtuple
from datetime import datetime
from multiprocessing import Process, Queue, Event
from pathlib import Path
from queue import Empty, Full

import numpy as np

# A flag that signals the processes to shut down
shutdown = Event()

# A list of subprocesses
processes = []

# A data structure passed between processes
Item = namedtuple("Item", ["data", "timestamp", "fresh_connection"])


class ConfigurationError(Exception):
    """An exception thrown when the config file is incorrectly specified"""


def signal_handler(sig, frame):
    """A handler for the Ctrl-C event and the TERM signal."""
    if shutdown.is_set() or sig == signal.SIGTERM:
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
        shutdown.set()


class TCPClient:
    """A TCP socket connection that reads newline-delimited messages."""

    def __init__(self, host, port, timeout=None):
        """Initialize the socket connection class.

        Args:
            host: IP address of the device
            port: integer port number to listen to
            timeout: a timeout in seconds for connecting and reading data (default: None)
        """
        self.host = host
        self.port = port
        self.timeout = timeout
        self._sock = None
        self._fd = None
        self._fresh = None

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    @property
    def fresh(self):
        """Indicates whether the connection is fresh, i.e. no data has been
        received over the socket yet.
        """
        return self._fresh

    def connect(self):
        """Establish socket connection, retrying if necessary
        """
        # Close any previously open socket-associated file descriptors
        self.close()

        logging.info(
            "Attempting to connect to socket at {}:{}...".format(self.host, self.port)
        )
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.settimeout(self.timeout)

        while not shutdown.is_set():
            try:
                self._sock.connect((self.host, self.port))
            except Exception:
                time.sleep(1)
            else:
                logging.info(
                    "Connected to {}:{}. Ready to receive device data...".format(
                        self.host, self.port
                    )
                )
                # Obtain a file descriptor capable of reading line by line
                self._fd = self._sock.makefile(mode="rb")
                # Mark that the connection has just been created
                self._fresh = True
                return

        # Shutting down before connection could be established

    def readline(self):
        """Read complete messages ending in "\n". If a partial message is received,
        buffer and wait for the remainder before continuing. If multiple joined
        messages are obtained, split them into individual records.

        Returns:
            data: a binary string

        Rases:
            OSError: propagate errors and empty messages as exceptions. There is no such
            thing as an empty message in TCP, so zero length means a peer disconnect.
        """
        try:
            data = self._fd.readline()
            if not data:
                raise ConnectionResetError("The device has closed the connection")
        except Exception as e:
            # Make the timeout message more elaborate instead of the default "timed out"
            if isinstance(e, socket.timeout):
                e = OSError(
                    "Read timed out. No messages received in {} seconds.".format(
                        self.timeout
                    )
                )
            raise e

        # Mark that some data has been successfully received over this connection.
        self._fresh = False

        return data

    def close(self):
        """Close all socket-associated handles."""
        try:
            if self._fd:
                self._fd.close()
            if self._sock:
                self._sock.shutdown(socket.SHUT_RDWR)
                self._sock.close()
        except Exception:
            pass

        self._fd = None
        self._sock = None


class Parser:
    """An implementation of the parser which extracts variables from the device
    binary messages and writes them periodically to disc."""

    def __init__(
        self, regex, var_names, multiplier, pack_length, destination,
    ):
        """Initialize the parser

        Args:
            regex, var_names, multiplier, pack_length: values from the config file
            destination: the target filename where to save the data, with an optional
                "{date}" placeholder for the current date and time.
        """
        self.regex = regex
        self.var_names = var_names
        self.all_vars = set(var_names + ["time"])
        self.multiplier = multiplier
        self.pack_length = pack_length
        self.destination = destination
        self._buffer = defaultdict(list)

    def extract(self, item):
        """Extract variables from the binary device data

        Args:
            item: a namedtuple containing the data, the timestamp and the fresh
                connection flag

        Returns:
            variables: a dict of variable-values pairs, including the timestamp

        Raises:
            AttributeError: when no match is found by the regex
            ValueError: when conversion of the extracted value to a float fails
            AssertionError: if the number of extracted values and var_names differ
            re.error: for other types of regex errors
        """
        try:
            match = re.match(self.regex, item.data)
            values = [float(value) * self.multiplier for value in match.groups()]
            assert len(values) == len(self.var_names), (
                "Regex extracted {} values, but {} var_names are specified"
            ).format(len(values), len(self.var_names))
        except AttributeError:
            # The regex pattern produced no match
            if item.fresh_connection:
                # We expect the very first message received upon establishing
                # a connection to be incomplete quite often.
                logging.debug("Possibly incomplete first message: {}".format(item.data))
            else:
                logging.error("Cannot parse a complete message: {}".format(item.data))
            raise
        except Exception as e:
            logging.error(e)
            raise
        else:
            variables = dict(zip(self.var_names, values))
            variables["time"] = item.timestamp
            logging.debug("Got {}".format(variables))

        return variables

    def write(self, variables):
        """Write the variables to an internal buffer, which is saved to disk
        when pack_length is reached.

        Args:
            variables: a dict of variable-value pairs, i.e. the output of extract()

        Raises:
            AssertionError: if the supplied variables differ from var_names + "time"
            Other exceptions: for filesystem-related and Numpy issues.
        """
        try:
            # Ensure that variable names are consistent across all messages
            all_vars = set(variables.keys())
            assert all_vars == self.all_vars, (
                "Cannot save the supplied variables. Expected {}, but got {}"
            ).format(sorted(self.all_vars), sorted(all_vars))
        except AssertionError as e:
            logging.error(e)
            raise

        # Collect the extracted values
        for var, value in variables.items():
            self._buffer[var].append(value)

        # Save the data to disk when the packing limit is reached
        if len(self._buffer["time"]) == self.pack_length:
            try:
                # Make sure the target directory exists
                p = Path(self.destination.format(date=datetime.utcnow()))
                p.parent.mkdir(parents=True, exist_ok=True)

                # Save the variables to a compressed Numpy file with a current timestamp
                np.savez_compressed(p, **self._buffer)
            except Exception as e:
                logging.error(
                    "Saving failed: {}. {:,} data points will be lost.".format(
                        e, self.pack_length
                    )
                )
                raise
            else:
                logging.info("Data saved to '{}'".format(p))
            finally:
                # Reset the in-memory storage
                self._buffer.clear()


def listen_device(queue, host, port, timeout):
    """Receive messages from the device over a TCP socket and queue them
    for parallel processing.

    Args:
        queue: a multiprocessing queue to send data to
        host: IP address of the device
        port: integer port number to listen to
        timeout: a timeout in seconds for connecting and reading data
    """
    with TCPClient(host, port, timeout) as client:
        # Establish socket connection to the device
        client.connect()

        while not shutdown.is_set():
            try:
                fresh_connection = client.fresh
                # Read device data line by line
                data = client.readline()
            except Exception as e:
                # Log the error and reconnect to the device
                logging.error(e)
                client.connect()
                continue

            # Get the current time for the received message. In a rare event that
            # multiple messages have been received over the socket at once, the
            # timestamps for individual messages will be very close to each other,
            # but not the same.
            timestamp = time.time()

            # Send the received data, the timestamp, and the connection state to the
            # second process for parsing
            try:
                item = Item(data, timestamp, fresh_connection)
                queue.put(item, block=False)
            except Full:
                logging.error(
                    "Queue is full, real-time data collection impossible. Exiting."
                )
                shutdown.set()


def process_data(queue, regex, var_names, multiplier, pack_length, destination):
    """Take messages from the queue, parse them and periodically save to disk.

    Args:
        queue: a multiprocessing queue to read messages from
        regex, var_names, multiplier, pack_length: values from the config file
        destination: the target filename where to save the data, with an optional
            "{date}" placeholder for the current date and time.
    """
    parser = Parser(regex, var_names, multiplier, pack_length, destination)

    # Loop until a shutdown flag is set and all items in the queue have been received
    while not (shutdown.is_set() and queue.empty()):
        try:
            item = queue.get(timeout=1)
        except Empty:
            # If the queue is empty, wait for messages that might arrive in the future
            continue

        try:
            variables = parser.extract(item)
            parser.write(variables)
        except Exception:
            continue


def read_cmdline():
    """Parse the command-line arguments.

    Returns:
        args: an object with the values of the command-line options
    """
    parser = argparse.ArgumentParser(description="Read and save device data.")
    # For better clarity, add a required block in the description
    required = parser.add_argument_group("required arguments")
    required.add_argument(
        "-c", "--config", help="path to the configuration file", required=True,
    )
    parser.add_argument(
        "--debug",
        help="turn on DEBUG logging (overrides the setting in the config file)",
        action="store_true",
    )
    args = parser.parse_args()
    return args


def load_config(f):
    """Load the configuration file with correct parameter data types.

    Args:
        f: a config file opened in text mode

    Returns:
        conf: a Namespace object with the loaded settings
    """
    # Interpolation is used e.g. for expanding the log file name
    config = configparser.ConfigParser(
        interpolation=configparser.ExtendedInterpolation()
    )
    config.read_file(f)

    def read_bytes(section, option):
        """Read an option from the config file as bytes"""
        value = config.get(section, option, raw=True)
        return literal_eval("b'{}'".format(value))

    # Handle the special ${date} variable
    date_format = config.get("parser", "date_format")
    config["DEFAULT"]["date"] = "{{date:{}}}".format(date_format)

    # Flatten the structure and convert the types of the parameters
    conf = dict(
        station_name=config.get("device", "station_name"),
        device_name=config.get("device", "device_name"),
        host=config.get("device", "host"),
        port=config.getint("device", "port"),
        timeout=config.getint("device", "timeout", fallback=None),
        regex=read_bytes("parser", "regex"),
        var_names=config.get("parser", "var_names").split(),
        multiplier=config.getfloat("parser", "multiplier"),
        pack_length=config.getint("parser", "pack_length"),
        destination=config.get("parser", "destination"),
        log_level=config.get("logging", "log_level"),
        log_file=config.get("logging", "log_file"),
    )

    # Convert the dictionary to a Namespace object, to enable .attribute access
    conf = argparse.Namespace(**conf)

    # Check if the regular expression is valid
    try:
        pattern = re.compile(conf.regex)
    except re.error as e:
        raise ConfigurationError("regex: {}".format(e))
    # Check that all capture groups are named
    if pattern.groups != len(conf.var_names):
        raise ConfigurationError(
            "mismatch between the number of regex capture groups and var_names"
        )

    # Ensure that "time" isn't used as a var_name in the config file
    if "time" in conf.var_names:
        raise ConfigurationError(
            "'time' is reserved for the message timestamp "
            "and cannot be listed in var_names"
        )

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
    formatter = logging.Formatter("%(levelname)-5s %(message)s")
    console.setFormatter(formatter)
    root.addHandler(console)


def main():
    # Parse the command-line arguments and load the config file
    args = read_cmdline()
    try:
        with open(args.config) as f:
            conf = load_config(f)
    except Exception as e:
        print("Failed to load configuration: {}".format(e))
        sys.exit(1)

    # Set up logging to the console and the log-files
    log_level = "DEBUG" if args.debug else conf.log_level
    configure_logging(log_level=log_level, log_file=conf.log_file)
    logging.info("Logging to the file '{}'".format(conf.log_file))

    # Ignore Ctrl-C in subprocesses
    signal.signal(signal.SIGINT, signal.SIG_IGN)

    # Create a communication queue between processes
    queue = Queue()

    # Launch the subprocesses
    p1 = Process(
        target=listen_device,
        kwargs=dict(queue=queue, host=conf.host, port=conf.port, timeout=conf.timeout),
    )
    p2 = Process(
        target=process_data,
        kwargs=dict(
            queue=queue,
            regex=conf.regex,
            var_names=conf.var_names,
            multiplier=conf.multiplier,
            pack_length=conf.pack_length,
            destination=conf.destination,
        ),
    )
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
