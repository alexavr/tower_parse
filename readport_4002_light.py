#!/usr/bin/env python3.5

station_name = "MSU"
sonic_name = "Test2"
HOST, PORT = "192.168.192.48", 4002

FillValue = -999.
pack_limit = 12000 # 12000 # 20*60*10 (10')

import socket #, os, sys
import re
from datetime import datetime
import numpy as np
import time
import struct

def parse_data(data_src):
    
    timestamp = time.time() 

    return timestamp 


def timestr():
    return '{0}'.format(datetime.utcnow().strftime('%Y-%m-%d_%H-%M-%S'))


def isOpen(ip,port):
   s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
   try:
      s.connect((ip, int(port)))
      s.shutdown(2)
      return True
   except:
      return False

fileout = "./data/"+station_name+"_"+sonic_name+"_"+timestr()
f = open(fileout+".bin", 'w+b')

sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.setblocking(True)
sock.connect((HOST, PORT))

try:
    while True:
        l = 1
        data_list = []
        while l <= pack_limit:
            
            try: 
                data_src = sock.recv(1024) #.decode()
            except (OSError):
                sock.close()
                while not isOpen(HOST, PORT):
                    print("MOXA socket %d is closed. Reinitiating..."%(PORT))
                    time.sleep(1)

                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.setblocking(True)
                sock.connect((HOST, PORT))
                data_src = sock.recv(1024) #.decode()

            data_list.append(parse_data(data_src)) 

            l += 1

        f.write(struct.pack('d'*len(data_list),*data_list))

        print("Output file is %s"%(fileout))

except KeyboardInterrupt as err:
    print('\n{0} :: Stopped {1}:{2} connection.'.format(timestr(), HOST, PORT))
finally:
    sock.close()
    f.close()


