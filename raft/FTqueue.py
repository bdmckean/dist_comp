#!/usr/bin/env python3

import subprocess
import sys
import os
import json

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
        self.res = os.popen('curl http://'+server+':'+port+'/v2/keys/'+label+'/elements -XPUT -d dir=true').read()
        print(self.res)
        num_elem = 0
        self.res = os.popen('curl http://'+server+':'+port+'/v2/keys/'+label+'/num_elem -XPUT -d value='+str(num_elem)).read()
        print(self.res)
        self.res = os.popen('curl http://'+server+':'+port+'/v2/keys/'+label+'/lock -XPUT -d value=0').read()
        print(self.res)

    def get_id(self):
        if (self.debug) : print("get_id") 
        return self.qid

    def delq(self):
        if (self.debug) : print("delq") 
        RaftQueue.qnum -= 1
        self.res = os.popen('curl http://127.0.0.1:12379/v2/keys/'+self.label+'?recursive=true -XDELETE').read()
        print(self.res)

    def get_num_elements(self):
        if (self.debug) : print("get_num_elem")
        self.res = os.popen('curl http://'+self.server+':'+self.port+'/v2/keys/'+self.label+'/num_elem -XGET').read()
        print(self.res)
        ans = json.loads(self.res)
        num = ans['node']['value']
        return int(num)

    def set_num_elements(self, num_elem):
        if (self.debug) : print("set_num_elem")
        self.res = os.popen('curl http://'+self.server+':'+self.port+'/v2/keys/'+self.label+'/num_elem -XPUT -d value='+str(num_elem)).read()
        print(self.res)

    def get_lock(self):
        #spin lock 
        while True:
            self.res = os.popen('curl http://'+self.server+':'+self.port+'/v2/keys/'+self.label+'/lock?prevValue=0 -XPUT -d value=1').read()
            print(self.res)
            if 'errorCode' not in self.res:
                break
                
    def release_lock(self):
        self.res = os.popen('curl http://'+self.server+':'+self.port+'/v2/keys/'+self.label+'/lock -XPUT -d value=0').read()
        print(self.res)

# wrapper functions
queue_list = {}
queue_list_by_label = {}

# Wrappers needing to lock
def create_Queue(label):
    rq = RaftQueue(label,'127.0.0.1','12379')
    queue_list[rq.qid] = rq
    queue_list_by_label[label] = rq
    # Create establishes lock
    return rq.qid

def del_Queue(qid):
    s = queue_list[qid]
    s.get_lock()
    s.delq()
    # No need to release lock

def push(qid, x):
    s = queue_list[qid]
    if (s.debug): print("push:",s,s.label,x)
    s.get_lock()
    num_elem = s.get_num_elements();
    print('curl http://'+s.server+':'+s.port+'/v2/keys/'+s.label+' -XPOST -d value='+str(x))
    s.res = os.popen('curl http://'+s.server+':'+s.port+'/v2/keys/'+s.label+'/elements -XPOST -d value='+str(x)).read()
    print(s.res)
    s.set_num_elements(num_elem + 1)
    s.release_lock()

def pop(qid):
    s = queue_list[qid]
    if (s.debug): print("pop:",s,s.label)
    s.get_lock()
    num_elem = s.get_num_elements();
    if num_elem == 0: return None
    s.res = os.popen('curl http://'+s.server+':'+s.port+'/v2/keys/'+s.label+'/elements').read()
    ans = json.loads(s.res)
    keys = ans['node']
    keys = keys['nodes']
    sorted_keys = keys.sort(key=lambda k: k['key'])
    value = keys[len(keys)-1]['value'] 
    key = keys[len(keys)-1]['key'] 
    print(key,value)
    print('curl http://'+s.server+':'+s.port+'/v2/keys'+key+' -XDELETE')
    s.res = os.popen('curl http://'+s.server+':'+s.port+'/v2/keys'+key+' -XDELETE').read()
    print(s.res)
    s.set_num_elements(num_elem - 1)
    s.release_lock()
    return value

# wrappers not needing a lock
def get_qid(label):
    return queue_list_by_label[label].qid

def top(qid):
    s = queue_list[qid]
    s.num_elements -= 1
    s.res = os.popen('curl http://'+s.server+':'+s.port+'/v2/keys/'+s.label+'/elements').read()
    ans = json.loads(s.res)
    keys = ans['node']
    keys = keys['nodes']
    sorted_keys = keys.sort(key=lambda k: k['key'])
    value = keys[len(keys)-1]['value'] 
    key = keys[len(keys)-1]['key'] 
    print(key,value)
    return value
  
def qsize(qid):
    s = queue_list[qid]
    num_elem = s.get_num_elements();
    return num_elem

def qshow(qid):
    s = queue_list[qid]
    res = os.popen('curl http://'+s.server+':'+s.port+'/v2/keys/'+s.label).read()
    print(res)
    res = os.popen('curl http://'+s.server+':'+s.port+'/v2/keys/'+s.label+'/elements').read()
    print(res)


# Test driver 
qid = create_Queue('rt')

push(qid,5)
push(qid,6)
push(qid,9)
push(qid,3)
qshow(qid)
print(qsize(qid))

x = 1000
x = pop(qid)

print(x)
qshow(qid)

del_Queue(qid)




