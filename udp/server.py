#!/usr/bin/env python3
# copyright 2016 Brian McKean
#UDP Server


import sys
import argparse, socket
from datetime import datetime

from statistics import mean, stdev

DEFAULT_PORT  = 10601
MAX_BYTES = 65535

SAMPLES =  5

def TimestampMillisec():
        return int((datetime.utcnow() - datetime(1970, 1, 1)).total_seconds() * 1000)

def client(port):
    out = []
    back = []
    offset = []
    delay = []
    mySocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    
    for i in range(SAMPLES): 
        #time1 = TimestampMillisec()
        time1 = datetime.now()
        text1 = 'The time is {}'.format(time1)
        #text1 = 'The time is {}'.format(time1)
        data = text1.encode('ascii')
        mySocket.sendto(data, (server,port))
        data, addr = mySocket.recvfrom(MAX_BYTES)
        text2 = data.decode('ascii')
        #time3 = TimestampMillisec()
        time3 = datetime.now()
        text3 = 'The time is {}'.format(time3)
        myAddr = mySocket.getsockname()
        print('The OS assigned me the address {}'.format(mySocket.getsockname()))
        print(text1) 
        print('The server at addr: {} replied {!r}'.format(addr, text2))
        print(text2)
        time1 = float((((text1.split())[-1]).split(":"))[-1])
        time2 = float((((text2.split())[-1]).split(":"))[-1])
        time3 = float((((text3.split())[-1]).split(":"))[-1])
        print('time 1 {:11.7f}, time 2 {}, time 3 {}'.format(time1,time2,time3))
        print("server={} client={}",server,myAddr)
        out.append(time2-time1)
        back.append(time3-time2)
        t1 = time1
        t2 = time2
        t3 = time2
        t4 = time3
        o = (.5)*((t2 - t1) + (t4 - t3)) 
        d = (t4 - t1 ) - (t3 - t2)
        offset.append(o) 
        delay.append(d)

    print(out)
    print(back)

    print("Out Mean={:11.7f}, stdev={:11.7f}".format( mean(out), stdev(out)))
    print("Back Mean={:11.7f}, stdev={:11.7f}".format( mean(back), stdev(back)))
    for i in range(SAMPLES):
        print ("sample:{}, offset{:11.7f}, delay{:11.7f}".format(i,offset[i],delay[i]))


# Set p server to server 
if ( len(sys.argv) < 2):
    #server = '128.138.201.66'
    #server = '67.164.172.138'
    server = '127.0.0.1'
    print("Using local host: ",end="");
else:
    server = str(sys.argv[1])
print("Using server ",server)
if ( len(sys.argv) > 2):
    port = int(sys.argv[2])
else:
    port = DEFAULT_PORT
print("Using port ",port)


client(port)
