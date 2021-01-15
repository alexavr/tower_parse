#!/usr/bin/env python

import argparse
import configparser
import logging
import logging.config
import signal
import socket
import sys
import time

try:
    # To enable advanced regular expressions: `pip install regex`
    import regex as re
except ImportError:
    import re

from ast import literal_eval
from collections import defaultdict, namedtuple
from datetime import datetime
from ipaddress import ip_address
from multiprocessing import Process, Queue, Event
from pathlib import Path
from queue import Empty, Full
from typing import AbstractSet, Any, Dict, Iterator, Optional, TextIO, Tuple, Union
from urllib.parse import urlparse

import numpy as np

# A flag that signals the processes to shut down
shutdown = Event()

# A list of subprocesses
processes = []

# A data structure passed between processes
Item = namedtuple("Item", ["data", "timestamp", "fresh_connection"])


class ConfigurationError(Exception):
    """An exception thrown when the config file is incorrectly specified"""


class ParseError(Exception):
    """An exception raised when the parser fails to process data or save it to disk"""


def signal_handler(sig, frame):  # noqa
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

    def __init__(self, host: str, port: int, timeout: Optional[float] = None):
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
    def fresh(self) -> bool:
        """Indicates whether the connection is fresh, i.e. no data has been
        received over the socket yet.
        """
        return self._fresh

    def connect(self):
        """Establish socket connection, retrying if necessary"""
        # Close any previously open socket-associated file descriptors
        self.close()

        logging.info(f"Attempting to connect to socket at {self.host}:{self.port}...")
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.settimeout(self.timeout)

        while not shutdown.is_set():
            try:
                self._sock.connect((self.host, self.port))
            except OSError:
                time.sleep(1)
            else:
                logging.info(
                    f"Connected to {self.host}:{self.port}. "
                    f"Ready to receive device data..."
                )
                # Obtain a file descriptor capable of reading line by line
                self._fd = self._sock.makefile(mode="rb")
                # Mark that the connection has just been created
                self._fresh = True
                return

        # Shutting down before connection could be established

    def readline(self) -> bytes:
        """Read complete messages ending in "\n". If a partial message is received,
        buffer and wait for the remainder before continuing. If multiple joined
        messages are obtained, split them into individual records.

        Returns:
            data: a binary string

        Raises:
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
                    f"Read timed out. No messages received in {self.timeout} seconds."
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
        except OSError:
            pass
        finally:
            self._fd = None
            self._sock = None


class Group:
    """Encapsulation of group_by related settings"""

    types = dict(int=int, float=float, str=lambda x: x.decode())

    def __init__(self, by: Optional[str] = None, dtype: Optional[str] = None):
        """Initialize the Group

        Args:
            by: the name of the grouping variable (default: None)
            dtype: the data type of the grouping variable (default: None)
        """
        self.by = by
        self.cast = self.types.get(dtype)

    @classmethod
    def from_config(cls, group_by: Union[str, None]) -> "Group":
        """Initialize the Group based on the configuration file value

        Args:
            group_by: the option value from the config

        Returns:
            group: an initialized instance of Group

        Raises:
            ConfigurationError: in case of ill-formatted group_by setting
        """
        by, dtype = None, None
        if group_by is not None:
            try:
                by, dtype = group_by.split(":")
            except ValueError:
                raise ConfigurationError(
                    "group_by must be in the format <variable>:<type>"
                )
        return cls(by, dtype)

    def __eq__(self, other: "Group"):
        """Test Group objects for equality"""
        if not isinstance(other, Group):
            # don't attempt to compare against unrelated types
            return NotImplemented
        return self.by == other.by and self.cast == other.cast

    def validate(self, variables: AbstractSet[str]):
        """Ensure that the Group variables are correctly specified

        Args:
            variables: the set of known variable names extracted by the regex

        Raises:
            ConfigurationError: if either group_by is not present in the set of extracted
                variable names, or the type is missing or specified incorrectly
        """
        if self.by is not None:
            if self.by not in variables:
                raise ConfigurationError(
                    f"group_by variable must by one of: {', '.join(variables)}"
                )
            if self.cast is None:
                raise ConfigurationError(
                    f"group_by type must be set to one of: "
                    f"{', '.join(self.types.keys())}"
                )


class Buffer:
    """A buffer that collects extracted variables by group, up to a packing limit"""

    def __init__(self, pack_length: int, group_by: Optional[str] = None):
        """Initialize the Buffer

        Args:
            pack_length: the number of records to save in each file
            group_by: the name of the grouping variable (default: None)
        """
        self.pack_length = pack_length
        self.group_by = group_by
        self._buf = dict()

    def put(self, extracted: Dict[str, Any]):
        """Collect the data separately for each of the groups, up to a packing limit

        Args:
            extracted: a dict of variable-value pairs, including the timestamp

        Raises:
            AssertionError: if basic consistency checks fail
        """
        group_value = extracted.get(self.group_by)
        buf = self._buf.get(group_value)
        if buf:
            assert extracted.keys() == buf.keys(), (
                f"Cannot buffer the supplied variables. "
                f"Expected {sorted(buf.keys())}, but got {sorted(extracted.keys())}"
            )

            # Checking the length of "time" doesn't alter the buffer, since the variable
            # must already be in it:
            assert "time" in buf, "'time' must be among supplied variables"
            assert (
                len(buf["time"]) < self.pack_length
            ), "Cannot add to a buffer that is already full"
        else:
            buf = self._buf[group_value] = defaultdict(list)

        # Collect the extracted values
        for var, value in extracted.items():
            buf[var].append(value)

    def full(self) -> Iterator[Tuple[Any, Dict[str, Any]]]:
        """Iterate over the groups that have reached the packing limit

        Yields:
            group_value: the value of the group that has pack_length items buffered
            buf: a dict of variable-list pairs, where each list is a vector of
                pack_length values.
        """
        for group_value, buf in self._buf.items():
            # Avoid creating an empty "time" variable when checking its length
            timestamps = buf.get("time")
            if timestamps and len(timestamps) == self.pack_length:
                yield group_value, buf

    def clear(self, group_value: Any):
        """Reset the in-memory buffer for a particular group

        Args:
            group_value: the value of the group to reset and start over
        """
        self._buf[group_value].clear()


class Parser:
    """An implementation of the parser which extracts variables from the device
    binary messages and writes them periodically to disk."""

    def __init__(
        self,
        regex: bytes,
        group: Group,
        pack_length: int,
        dest: Union[str, Path],
    ):
        """Initialize the parser

        Args:
            regex: regular expression for variable extraction
            group: an instance of Group, containing group_by related settings
            pack_length: the number of records to save in each file
            dest: the target filename where to save the data, with an optional
                "{date}" placeholder for the current date and time.
        """
        self.regex = regex
        self.group = group
        self.dest = dest
        self._buffer = Buffer(pack_length, group.by)
        # Convert all variables to float, except for the group.by variable, if any
        self._cast = defaultdict(lambda: float)
        self._cast[group.by] = group.cast

    def extract(self, item: Item) -> Dict[str, Any]:
        """Extract variables from the binary device data

        Args:
            item: a namedtuple containing the data, the timestamp and the fresh
                connection flag

        Returns:
            extracted: a dict of variable-value pairs, including the timestamp

        Raises:
            AttributeError: when no match is found by the regex
            ValueError or UnicodeDecodeError: type conversion of extracted values fails
            re.error: for other types of regex errors
        """
        try:
            match = re.match(self.regex, item.data)
            # Collect the results, converting to appropriate data types and filtering out
            # capture groups that didn't match
            extracted = {
                key: self._cast[key](value)
                for key, value in match.groupdict().items()
                if value is not None
            }
        except AttributeError as e:
            # The regex pattern produced no match
            if item.fresh_connection:
                # We expect the very first message received upon establishing
                # a connection to be incomplete quite often.
                logging.debug(f"Possibly incomplete first message: {item.data}")
            else:
                logging.error(f"Cannot parse the message: {item.data}")
            raise ParseError(e)
        except Exception as e:
            logging.error(e)
            raise ParseError(e)
        else:
            extracted["time"] = item.timestamp
            logging.debug(f"Got {extracted}")

        return extracted

    def write(self, extracted: Dict[str, Any]):
        """Write the extracted variables to an internal buffer, which is saved to disk
        when pack_length is reached.

        Args:
            extracted: a dict of variable-value pairs, i.e. the output of extract()

        Raises:
            AssertionError: if the supplied variables differ from those previously saved
            Other exceptions: for filesystem-related and NumPy issues.
        """
        try:
            self._buffer.put(extracted)
        except AssertionError as e:
            logging.error(e)
            raise ParseError(e)

        # Save the data to disk when the packing limit is reached
        for group_value, vectors in self._buffer.full():
            try:
                # Make sure the destination directory exists
                group = group_value if group_value is not None else ""
                target = Path(
                    str(self.dest).format(group=group, date=datetime.utcnow())
                )
                target.parent.mkdir(parents=True, exist_ok=True)

                # Save the variables to a temporary file
                tmp_file = target.with_suffix(".tmp")
                with tmp_file.open(mode="wb") as f:
                    np.savez_compressed(f, **vectors)

                # Rename to ".npz" to make `rsync --remove-source-files` safe
                tmp_file.rename(target)
            except Exception as e:
                logging.error(
                    f"Saving failed: {e}. "
                    f"{self._buffer.pack_length:,} data points will be lost."
                )
                raise ParseError(e)
            else:
                logging.info(f"Data saved to '{target}'")
            finally:
                # Reset the in-memory storage
                self._buffer.clear(group_value)


def listen_device(queue: Queue, host: str, port: int, timeout: Optional[float] = None):
    """Receive messages from the device over a TCP socket and queue them
    for parallel processing.

    Args:
        queue: a multiprocessing queue to send data to
        host: IP address of the device
        port: integer port number to listen to
        timeout: a timeout in seconds for connecting and reading data (default: None)
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


def process_data(
    queue: Queue, regex: bytes, group: Group, pack_length: int, dest: Union[str, Path]
):
    """Take messages from the queue, parse them and periodically save to disk.

    Args:
        queue: a multiprocessing queue to read messages from
        regex: regular expression for variable extraction
        group: an instance of Group, containing group_by related settings
        pack_length: the number of records to save in each file
        dest: the target filename where to save the data, with an optional
            "{date}" placeholder for the current date and time.
    """
    parser = Parser(regex, group, pack_length, dest)

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
        except ParseError:
            continue


def read_cmdline() -> argparse.Namespace:
    """Parse the command-line arguments.

    Returns:
        args: an object with the values of the command-line options
    """
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description="Read device data over a TCP socket connection.",
        epilog="""Examples of usage:
  Parse and save device data to NumPy archives:
    $ ./readport.py --config readport_4001.conf
    
  Save binary messages from the device to a file. Useful when the format isn't yet known:
    $ ./readport.py --echo 192.168.192.48:4001 > data.bin
""",
    )
    # For better clarity, add a required block in the description
    required = parser.add_argument_group("required arguments (one of)")
    either = required.add_mutually_exclusive_group(required=True)
    either.add_argument(
        "-c",
        "--config",
        help="path to the configuration file",
    )
    either.add_argument(
        "--echo",
        metavar="IP:PORT",
        help="print messages coming from a specified address to stdout",
    )
    parser.add_argument(
        "--debug",
        help="turn on DEBUG logging (overrides the setting in the config file)",
        action="store_true",
    )
    args = parser.parse_args()
    return args


def load_config(f: TextIO) -> argparse.Namespace:
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

    # Read regex as a byte-string
    regex = literal_eval("b'{}'".format(config.get("parser", "regex", raw=True)))
    variables = validate_regex(regex)

    # Load group_by related options
    group = Group.from_config(config.get("parser", "group_by", fallback=None))
    group.validate(variables)

    # Hardcode the filename template, with {group} and {date} to be substituted when
    # writing to disk.
    config["DEFAULT"][
        "filename"
    ] = "${device:station}_${device:name}{group}_{date:%Y-%m-%d_%H-%M-%S}.npz"

    # Flatten the structure and convert the types of the parameters
    conf = dict(
        station=config.get("device", "station"),
        device=config.get("device", "name"),
        host=config.get("device", "host"),
        port=config.getint("device", "port"),
        timeout=config.getint("device", "timeout", fallback=None),
        regex=regex,
        group=group,
        pack_length=config.getint("parser", "pack_length"),
        dest_dir=config.get("parser", "destination"),
        filename=config.get("DEFAULT", "filename"),
        log_level=config.get("logging", "level"),
        log_file=config.get("logging", "file"),
    )

    # Convert the dictionary to a Namespace object, to enable .attribute access
    conf = argparse.Namespace(**conf)

    return conf


def validate_regex(regex: bytes) -> AbstractSet[str]:
    """Check if the regular expression is valid

    Args:
        regex: regular expression for variable extraction

    Returns:
        variables: a set of variable names captured by the regex

    Raises:
        ConfigurationError: in case of obvious issues with the regex
    """
    try:
        pattern = re.compile(regex)
    except re.error as e:
        # Additional functionality is supported with a 3rd-party regex module, e.g.:
        if "redefinition of group name" in e.msg:
            e.args = (
                e.args[0] + "\nTo support such advanced regex functionality, "
                "please `pip install regex`.",
            ) + e.args[1:]
        raise ConfigurationError(f"regex: {e}")

    if pattern.groups != len(pattern.groupindex):
        raise ConfigurationError("all of the regex capture groups must be named")

    # Ensure that "time" isn't used in the regex
    if "time" in pattern.groupindex:
        raise ConfigurationError(
            "don't use 'time' as a regex variable, "
            "it is reserved for the message timestamp"
        )

    return pattern.groupindex.keys()


def configure_logging(level: Optional[str] = "INFO", file: Optional[str] = None):
    """Setup rotated logging to the file and the console

    Args:
        level: the threshold for the logging system (default: "INFO")
        file: the filename of the log to write to (default: None)
    """
    logging_conf = {
        "version": 1,
        "formatters": {
            "timestamped": {
                "class": "logging.Formatter",
                "format": "%(asctime)s [%(levelname)s]: %(message)s",
            },
            "concise": {
                "class": "logging.Formatter",
                "format": "%(levelname)-5s %(message)s",
            },
        },
        "handlers": {
            # Setup a rotating log file. At most 5 backup copies are kept, <10 MB each.
            "file": {
                "class": "logging.handlers.RotatingFileHandler",
                "filename": file,
                "mode": "a",
                "maxBytes": int(1e7),
                "backupCount": 5,
                "formatter": "timestamped",
            },
            # Setup simultaneous logging to the console (stderr)
            "console": {
                "class": "logging.StreamHandler",
                "formatter": "concise",
            },
        },
        "root": {
            "level": level,
            "handlers": ["console", "file"] if file else ["console"],
        },
    }
    if not file:
        del logging_conf["handlers"]["file"]

    logging.config.dictConfig(logging_conf)


def echo(host: str, port: int):
    """Connect to the device and print incoming messages to stdout

    Args:
        host: IP address of the device
        port: integer port number to listen to
    """
    with TCPClient(host, port) as client:
        # Establish socket connection to the device
        client.connect()

        while True:
            try:
                # Read device data line by line
                data = client.readline()
            except Exception as e:
                logging.error(e)
                return
            else:
                # Ideally, the user will redirect stdout to a file to record binary
                # messages and avoid corrupting the terminal
                sys.stdout.buffer.write(data)
                sys.stdout.buffer.flush()


def parse(conf: argparse.Namespace):
    """Launch long-running processes to listen, parse, and save incoming data

    Args:
        conf: all of the loaded config file settings
    """
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
            group=conf.group,
            pack_length=conf.pack_length,
            dest=Path(conf.dest_dir) / conf.filename,
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


def main():
    # Parse the command-line arguments
    args = read_cmdline()
    # Log to stderr only, until the config file has been loaded
    configure_logging()

    if args.echo:
        # Obtain the IP and port number of the device
        try:
            parsed = urlparse(f"tcp://{args.echo}")
            host = str(ip_address(parsed.hostname))
            port = parsed.port
            assert host, "please provide a valid IP address"
            assert port, "please provide a valid port number"
        except (ValueError, AssertionError) as e:
            logging.error(f"Failed to parse {args.echo!r} as IP:PORT: {e}")
            sys.exit(1)

        try:
            # Connect to the device and print incoming messages to stdout
            echo(host, port)
        except KeyboardInterrupt:
            pass

    else:
        # Load the config file
        try:
            with open(args.config) as f:
                conf = load_config(f)
        except Exception as e:
            logging.error(f"Failed to load configuration: {e}")
            sys.exit(1)

        # Set up logging to the console and the log-file
        log_level = "DEBUG" if args.debug else conf.log_level
        configure_logging(level=log_level, file=conf.log_file)
        logging.info(f"Logging to the file '{conf.log_file}'")

        # Launch long-running processes to listen, parse, and save incoming data
        parse(conf)


if __name__ == "__main__":
    main()
