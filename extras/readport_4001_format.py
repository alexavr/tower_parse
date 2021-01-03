#!/usr/bin/env python3.5

import socket

station_name = "MSU"
device_name = "Test1"
# HOST, PORT = "192.168.192.48", 4001
HOST, PORT = "192.168.192.48", 4001

template = "./data/{station_name}_{device_name}_{idx}.bin"

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
    sock.connect((HOST, PORT))

    for i in range(3):
        data = sock.recv(1024)

        print("Final message byte:", hex(data[-1]))

        filename = template.format(
            station_name=station_name, device_name=device_name, idx=i
        )
        with open(filename, "wb") as f:
            f.write(data)
        print("Binary message saved to {}".format(filename))
