#!/usr/bin/env python3.5

station_name = "MSU"
device_name = "Test1"
HOST, PORT = "192.168.192.48", 4001

FillValue = -999.
pack_limit = 12000 # 12000 # 20*60*10 (10')

import socket, time #, os, sys
import re
from datetime import datetime
import numpy as np
import time

def parse_data(data_src):
    
    pattern = r"^.+,([^,]+),([^,]+),([^,]+),.,([^,]+),([^,]+),([^,]+)$"
    
    timestamp = time.time() # datetime.utcnow().timestamp()
    # import pdb; pdb.set_trace()
    match = re.match(pattern, data_src)
    
    if match:
        data_str = match.group(1, 2, 3, 4)                                      ### u, v, w, t
        try:
            u, v, w, t = [float(value) for value in data_str]
        except ValueError:
            u, v, w, t = [FillValue] * 4                     
    else:
        u, v, w, t = [FillValue] * 4                         

    return timestamp, u, v, w, t


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


sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.setblocking(True)
sock.connect((HOST, PORT))

try:
    while True:
        l = 1
        data_list = []
        while l <= pack_limit:
            
            try: 
                data_src = sock.recv(1024).decode()
#                print(data_src)
            except (UnicodeDecodeError, OSError):
                sock.close()
                while not isOpen(HOST, PORT):
                    print("MOXA socket %d is closed. Reinitiating..."%(PORT))
                    time.sleep(1)

                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.setblocking(True)
                sock.connect((HOST, PORT))
                data_src = sock.recv(1024).decode()

            data_list.append(parse_data(data_src)) 

#            print("received message: line = {}; time = {}; u = {}; v = {}; w = {}; t = {}; ".format(l, *data_list[-1]))
            
            l += 1

        fileout = "./data/"+station_name+"_"+device_name+"_"+timestr()

        # # ASCII
        # with open(fileout+".dat", 'w') as f:
        #     for item in data_list:
        #         output_str = ",".join([str(f) for f in item])
        #         f.write("%s\n" % output_str)

        # # PICLE
        # with open(fileout+".pkl", 'wb') as f:
        #     pickle.dump(data_list,f,protocol=4) 

        # # MSGPack
        # with open(fileout+".msgp", 'wb') as f:
        #     msgpack.dump(data_list,f,use_bin_type=True) 

        # NumPy
        data_np = np.array(data_list)
        # timestamp, u, v, w, t = *data_np.T
        timestamp = data_np[:,0]
        u = data_np[:,1]
        v = data_np[:,2]
        w = data_np[:,3]
        t = data_np[:,4]
        np.savez_compressed(fileout+".npz",time=timestamp, u=u, v=v, w=w, temp=t)
        # np.save(fileout+".npy",data_np)

        print("Output file is %s"%(fileout))

except KeyboardInterrupt as err:
    print('\n{0} :: Stopped {1}:{2} connection.'.format(timestr(), HOST, PORT))
finally:
    sock.close()


