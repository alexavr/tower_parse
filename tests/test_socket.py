import logging
import queue
import re
import socket
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from typing import List

import pytest
from readport import shutdown, listen_device

HOST, PORT = "127.0.0.1", 9999


class TCPServer(ExitStack):
    """A TCP socket server for testing. Meant to be used as a context manager.
    """

    def __init__(self, host: str, port: int):
        super().__init__()
        self.host = host
        self.port = port
        self.sock = None
        self.queue = queue.Queue()
        self.log = logging.getLogger("TCPServer")

    def __enter__(self):
        super().__enter__()
        self.log.debug("starting")
        # Automatically close the socket when exiting the context manager,
        # even if run() has not been executed.
        self.sock = self.enter_context(
            socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        )
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind((self.host, self.port))
        self.sock.listen()
        self.log.debug("listening on {}".format(self.sock.getsockname()))
        return self

    def close(self):
        if self.sock:
            self.log.debug("closing the socket")
            self.sock.close()
            self.sock = None

    def run(self):
        """Run the main event loop that processes the messages and executes instructions.

        Currently supported instructions are:
            <sleep ??>: introduce a delay of ?? seconds before the next message
            <disconnect>: close the connection with the client
            <shutdown>: gracefully terminate both the TCP server and the client

        Everything else is sent to the client as a message.
        """
        shutdown.clear()
        while not shutdown.is_set():
            # while self.sock:
            self.log.debug("connecting")
            conn, addr = self.sock.accept()
            self.log.debug("connected to {}".format(addr))

            with conn:
                while True:
                    try:
                        msg = self.queue.get(timeout=1)
                    except queue.Empty:
                        continue

                    m = re.match(br"<sleep (\d+)>", msg)
                    if m:
                        duration = int(m.group(1))
                        self.log.debug("sleeping for {} seconds".format(duration))
                        time.sleep(duration)
                    elif msg == b"<disconnect>":
                        break
                    elif msg == b"<shutdown>":
                        # Wait for the client to process the remaining messages
                        time.sleep(0.2)
                        # Shutdown both the server and the client
                        shutdown.set()
                        break
                    else:
                        self.log.debug("sending {!r}".format(msg))
                        try:
                            conn.sendall(msg)
                        except Exception as e:
                            self.log.error(e)
                            break

                self.log.debug("closing connection")

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
    # Use concurrent.futures.ThreadPoolExecutor instead of threading.Thread
    # to propagate exceptions to the caller.
    with ThreadPoolExecutor(max_workers=1) as executor, TCPServer(HOST, PORT) as srv:
        future = executor.submit(srv.run)
        yield srv
        # Wait for the task to complete
        future.result()
    logging.debug("TCPServer stopped")


def test_connection(server):
    """Check that the TCP server and a basic reconnecting client are communicating properly.
    """
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
                log.debug("connected to {}".format(sock.getpeername()))
            except (OSError, ConnectionRefusedError) as e:
                log.error(e)
                break

            while True:
                try:
                    data = sock.recv(1024)
                    if not data:
                        raise ConnectionResetError("connection closed by the server")
                except OSError as e:
                    log.error(e)
                    break
                else:
                    log.debug("Received {!r}".format(data))
                    # The messages may arrive bundled together. Split and reformat them.
                    received.extend([b"%s\n" % b for b in data.strip().split(b"\n")])

    log.debug("received: {}".format(received))
    assert received == expected


def test_listen_device_readline(server):
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
    server.send(outgoing)

    q = queue.Queue()
    listen_device(q, HOST, PORT, timeout=None)

    received = []
    while not q.empty():
        msg, timestamp, fresh_conn = q.get()
        received.append(msg)

    logging.debug("Received: {}".format(received))
    assert received == expected


@pytest.mark.skip
def test_listen_device_timeout(server):
    instructions = [
        b"message 1\n",
        b"<disconnect>",
        b"message 2\n",
        b"<sleep 2>",
        b"message 3\n",
        b"<shutdown>",
    ]
    expected = [
        b"message 1\n",
        b"message 2\n",
        b"message 3\n",
    ]
    server.send(instructions)

    q = queue.Queue()
    listen_device(q, HOST, PORT, timeout=1)

    received = []
    while not q.empty():
        msg, timestamp, fresh_conn = q.get()
        received.append(msg)

    logging.debug("Received: {}".format(received))
    assert received == expected
