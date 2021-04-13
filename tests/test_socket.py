import logging
import queue
import re
import socket
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from typing import List

import pytest
from readport import echo, listen_device, shutdown

HOST, PORT = "127.0.0.1", 9999


def humanize(address):
    """Format the network address tuple (host and port) as a string"""
    host, port = address
    return f"{host}:{port}"


class TCPServer(ExitStack):
    """A TCP socket server for testing. Meant to be used as a context manager."""

    def __init__(self, host: str, port: int):
        super().__init__()
        self.host = host
        self.port = port
        self.sock = None
        self.queue = queue.Queue()
        self.log = logging.getLogger("TCPServer")

    def __enter__(self):
        super().__enter__()
        self.log.debug("Starting")
        # Automatically close the socket when exiting the context manager,
        # even if run() has not been executed.
        self.sock = self.enter_context(
            socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        )
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind((self.host, self.port))
        self.sock.listen()
        self.log.debug(f"Listening on {humanize(self.sock.getsockname())}")
        return self

    def close(self):
        if self.sock:
            self.log.debug("Closing the socket")
            self.sock.close()
            self.sock = None

    def run(self):
        """Run the main event loop that processes the messages and executes instructions.

        Currently supported instructions are:
            <timeout ??>: introduce a delay of ?? seconds and drop the connection
            <disconnect>: close the connection with the client
            <shutdown>: gracefully terminate both the TCP server and the client

        Everything else is sent to the client as a message.
        """
        shutdown.clear()
        while not shutdown.is_set():
            self.log.debug("Connecting")
            conn, addr = self.sock.accept()
            self.log.debug(f"Connected to {humanize(addr)}")

            with conn:
                while True:
                    try:
                        msg = self.queue.get(timeout=1)
                    except queue.Empty:
                        continue

                    m = re.match(br"<timeout (\d+[.]?\d*)>", msg)
                    if m:
                        duration = float(m.group(1))
                        self.log.debug(f"Sleeping for {duration} second(s)")
                        time.sleep(duration)
                        break
                    elif msg == b"<disconnect>":
                        break
                    elif msg == b"<shutdown>":
                        # Wait for the client to process the messages. This doesn't
                        # guarantee anything, but simplifies the code significantly.
                        time.sleep(0.5)
                        # Shutdown both the server and the client
                        shutdown.set()
                        break
                    else:
                        self.log.debug(f"Sending {msg!r}")
                        try:
                            conn.sendall(msg)
                        except Exception as e:
                            self.log.error(e)
                            break

                self.log.debug("Closing connection")

        # Close the socket manually to prevent the client from reconnecting
        self.close()

    def send(self, sequence: List[bytes]):
        """Enqueue a sequence of messages or instructions for the server to process and send.

        Args:
            sequence: a list of byte-string messages or instructions
        """
        for msg in sequence:
            self.queue.put(msg)


@pytest.fixture
def server():
    """Launch a TCP server in a separate thread and return the instantiated object."""
    # Use concurrent.futures.ThreadPoolExecutor instead of threading.Thread
    # to propagate exceptions to the caller.
    with ThreadPoolExecutor(max_workers=1) as executor, TCPServer(HOST, PORT) as srv:
        future = executor.submit(srv.run)
        yield srv
        # Wait for the task to complete
        future.result()
    logging.debug("TCPServer stopped")


@pytest.fixture
def store():
    """Record all objects sent to a queue and return the individual attributes."""

    class Store:
        def __init__(self):
            self.queue = queue.Queue()
            self._received = []

        def __getattr__(self, name):
            while not self.queue.empty():
                self._received.append(self.queue.get())
            return [getattr(item, name) for item in self._received]

        def reset(self):
            self._received = []

    yield Store()


def test_connection(server):
    """Check that the TCP server and a basic reconnecting client are communicating properly."""
    outgoing = [
        b"message 1\n",
        b"message 2\n",
        b"<disconnect>",
        b"message 3\n",
        b"message 4\n",
        b"<shutdown>",
    ]
    expected = [b"message 1\n", b"message 2\n", b"message 3\n", b"message 4\n"]
    server.send(outgoing)

    log = logging.getLogger("TCPClient")

    received = []
    while True:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            try:
                sock.settimeout(5)
                sock.connect((HOST, PORT))
                log.debug(f"Connected to {humanize(sock.getpeername())}")
            except (OSError, ConnectionRefusedError) as e:
                log.error(e)
                break

            while True:
                try:
                    data = sock.recv(1024)
                    if not data:
                        raise ConnectionResetError("Connection closed by the server")
                except OSError as e:
                    log.error(e)
                    break
                else:
                    log.debug(f"Received {data!r}")
                    # The messages may arrive bundled together. Split and reformat them.
                    received.extend([b"%s\n" % b for b in data.strip().split(b"\n")])

    log.debug(f"Received: {received}")
    assert received == expected


def test_listen_device_readline(server, store):
    """Ensure that the messages are read line by line"""
    outgoing = [
        b"message 1\n",
        b"mess",
        b"age 2\n",
        b"message 3",
        b"\n",
        b"<disconnect>",
        b"",
        b"message 4\n",
        b"message 5\nmessage 6\n",
        b"<shutdown>",
    ]
    expected = [
        b"message 1\n",
        b"message 2\n",
        b"message 3\n",
        b"message 4\n",
        b"message 5\n",
        b"message 6\n",
    ]
    expected_fresh_conn = [True, False, False, True, False, False]
    server.send(outgoing)
    listen_device(store.queue, HOST, PORT, timeout=None)

    logging.debug(f"Received: {store.data}")
    assert store.data == expected
    assert store.fresh_connection == expected_fresh_conn
    # Ensure that the timestamps are monotonically increasing
    assert all(t1 < t2 for t1, t2 in zip(store.timestamp, store.timestamp[1:]))


def test_listen_device_timeout(server, store, caplog):
    """Check that the timeout triggers reconnection and receives the follow-up messages"""
    instructions = [
        b"message 1\n",
        b"<timeout 1>",
        b"message 2\n",
        b"<shutdown>",
    ]
    expected = [
        b"message 1\n",
        b"message 2\n",
    ]
    server.send(instructions)
    listen_device(store.queue, HOST, PORT, timeout=0.75)

    logging.debug(f"Received: {store.data}")
    assert store.data == expected
    # Make sure that there was exactly one timeout in the logs
    timeout_logs = [rec.message for rec in caplog.records if "timed out" in rec.message]
    assert len(timeout_logs) == 1


def test_echo(server, capsysbinary):
    """Verify that echo is working properly"""
    instructions = [
        b"message 1\n",
        b"message 2\n",
        b"<shutdown>",
    ]
    expected = b"message 1\nmessage 2\n"

    server.send(instructions)
    echo(HOST, PORT)  # will return when connection is closed, does not reconnect

    captured = capsysbinary.readouterr()
    assert captured.out == expected
