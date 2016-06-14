#!/usr/bin/env python3
# copyright 2016 Brian McKean
#UDP Server


import sys
import argparse, socket
from datetime import datetime

DEFAULT_PORT  = 10601
MAX_BYTES = 65535

def TimestampMillisec():
    return int((datetime.utcnow() - datetime(1970, 1, 1)).total_seconds() * 1000)
        


def server(port):
    mySocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    mySocket.bind((client,port))
    print('Listing at {}'.format(mySocket.getsockname())) 
    while 1:
        data1, addr = mySocket.recvfrom(MAX_BYTES)
        text1 = data1.decode('ascii')
        text2 = 'The time is {}'.format(datetime.now())
        #text2 = 'The time is {}'.format(TimestampMillisec())
        data2 = text2.encode('ascii')
        mySocket.sendto(data2, addr)
        print('The client at addr: {} sent {!r}'.format(addr, text1))
        print(text2);

# Set p server to client 
if ( len(sys.argv) < 2):
    client = '127.0.0.1'
    #client = '10.201.13.239'    
    print("Using local host: ",end="");
else:
    client = str(sys.argv[1])
print("Using client ",client)
if ( len(sys.argv) > 2):
    port = int(sys.argv[2])
else:
    port = DEFAULT_PORT
print("Using port ",port)


server(port)
