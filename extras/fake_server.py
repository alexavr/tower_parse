import argparse
import random
import signal
import socket
import time

HOST = "127.0.0.1"  # Standard loopback interface address (localhost)
PORT = 4001  # Port to listen on (non-privileged ports are > 1023)


def keyboardInterruptHandler(signal, frame):
    print("KeyboardInterrupt caught. Exiting")
    exit(0)


class Generator:
    """Generate random messages with appropriate format and sequential IDs"""

    def __init__(self, broken=False):
        """Initialize the generator.

        Args:
            broken: if True, send broken messages, split between sends (default: {False})
        """
        # Return a variable-length message of the form:
        # b'\x02Q,+000.079,-000.102,+000.095,M,+014.94,0000001,\x030F\r\n'
        # Note: In the original message from the device, message ID is always "00"
        self.template = (
            "\x02Q,{:+0{w}.{p}f},{:+0{w}.{p}f},{:+0{w}.{p}f},"
            "M,{:+0{w}.{p}f},{message_id:07d},\x03{suffix:02X}\r\n"
        )
        self.message_id = 0
        self.broken = broken
        self.buffer = b""

    def get_data(self):
        precision = random.choice([2, 3])
        width = 5 + precision
        floats = [random.uniform(-99.99, 99.99) for _ in range(4)]
        suffix = random.randint(0, 255)
        data = self.template.format(
            *floats, message_id=self.message_id, suffix=suffix, w=width, p=precision
        )
        data = data.encode("ascii")

        if self.broken:
            chunk_length = random.randint(1, len(data) - 1)

            if self.message_id == 0:
                # Simulate an incomplete message
                data = data[-chunk_length:]
            else:
                part1, part2 = data[:chunk_length], data[chunk_length:]
                # Send just the first part of the message now, including the buffer,
                # saving the second part for later.
                data = self.buffer + part1
                self.buffer = part2

        self.message_id += 1
        return data


def read_cmdline():
    """Parse the command-line arguments.

    Returns:
        args: an object with the values of the command-line options
    """
    parser = argparse.ArgumentParser(
        description=(
            "A server that sends data over the socket. "
            "Used for testing the readport.py client."
        )
    )
    parser.add_argument(
        "-f",
        "--frequency",
        help=(
            "Approximate number of messages per second to send (default: 20). "
            "Use 0 to send at a maximum possible rate."
        ),
        default=20,
        type=float,
    )
    parser.add_argument(
        "-b",
        "--broken",
        help=(
            "Simulate a broken / streaming server by sending "
            "partial or incomplete messages"
        ),
        action="store_true",
    )
    args = parser.parse_args()
    return args


def main():
    signal.signal(signal.SIGINT, keyboardInterruptHandler)

    args = read_cmdline()
    frequency, broken = args.frequency, args.broken

    if frequency == 0:
        delay = 0
        freq_desc = "an unlimited number of messages per second"
    else:
        delay = 1 / frequency
        freq_desc = f"approximately {frequency:,} message(s) per second"

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        sock.bind((HOST, PORT))
        sock.listen()
        print(f"Listening on {HOST}:{PORT}")

        while True:
            conn, addr = sock.accept()
            with conn:
                print(f"Connected by {addr[0]}:{addr[1]}")
                print(f"Sending {freq_desc}...")
                generator = Generator(broken)
                while True:
                    data = generator.get_data()
                    try:
                        conn.sendall(data)
                        time.sleep(delay)
                    except (BrokenPipeError, ConnectionResetError):
                        print("Connection lost. Waiting for new connection.")
                        break


if __name__ == "__main__":
    main()
