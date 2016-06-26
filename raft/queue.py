#!/usr/bin/env python3

import subprocess
import sys
import os


class RaftQueue(object):
    debug = 1;
    qnum = 0

    def __init__(self,label,server,port):
        self.debug = 0
        if (RaftQueue.debug) : 
            print("__init__(",server,port) 
            self.debug = 1 
        self.server = server
        self.port = port
        self.num_elements = 0
        self.label = label
        self.qid = RaftQueue.qnum + 1
        RaftQueue.qnum += 1
        print('curl http://'+server+':'+port+'/v2/keys/'+label+' -XPUT -d dir=true')
        self.res = os.popen('curl http://'+server+':'+port+'/v2/keys/'+label+' -XPUT -d dir=true').read()
        print(self.res)

    def get_id(self):
        if (self.debug) : print("get_id") 
        return self.qid

    def delq(self):
        if (self.debug) : print("delq") 
        RaftQueue.qnum -= 1
        self.res = os.popen('curl http://127.0.0.1:12379/v2/keys/'+self.label+'?recursive=true -XDELETE').read()
        print(self.res)
        
# wrapper functions
queue_list = {}
queue_list_by_label = {}


def create_Queue(label):
    rq = RaftQueue(label,'127.0.0.1','12379')
    queue_list[rq.qid] = rq
    queue_list_by_label[label] = rq
    return rq.qid

def del_Queue(qid):
    s = queue_list[qid]
    s.delq()

def push(qid, x):
    s = queue_lists[qid]
    s.num_elements += 1
    s.res = os.popen('curl http://'+s.server+':'+s.port+'/v2/keys/'+s.label+' -XPOST -d value='+str(x))

def pop(qid):
    s = queue_lists[qid]
    s.num_elements -= 1

def get_qid(label):
    return queue_list_by_label[label].qid

def top(qid):
    s = queue_lists[qid]
  
def qsize(qid):
    s = queue_lists[qid]
    return s.num_elements


qid = create_Queue('rt')

del_Queue(qid)




