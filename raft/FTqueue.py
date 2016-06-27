#!/usr/bin/env python3

import subprocess
import sys
import os
import json

class RaftQueue(object):
    debug = 0;
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
        self.res = os.popen('curl -s http://'+server+':'+port+'/v2/keys/'+label+' -XPUT -d dir=true').read()
        if (RaftQueue.debug) : print(self.res)
        self.res = os.popen('curl -s http://'+server+':'+port+'/v2/keys/'+label+'/elements -XPUT -d dir=true').read()
        if (RaftQueue.debug) : print(self.res)
        num_elem = 0
        self.res = os.popen('curl -s http://'+server+':'+port+'/v2/keys/'+label+'/num_elem -XPUT -d value='+str(num_elem)).read()
        if (RaftQueue.debug) : print(self.res)
        self.res = os.popen('curl -s http://'+server+':'+port+'/v2/keys/'+label+'/lock -XPUT -d value=0').read()
        if (RaftQueue.debug) :  print(self.res)

    def get_id(self):
        if (self.debug) : print("get_id") 
        return self.qid

    def delq(self):
        if (self.debug) : print("delq") 
        self.get_lock()
        RaftQueue.qnum -= 1
        self.res = os.popen('curl -s http://127.0.0.1:12379/v2/keys/'+self.label+'?recursive=true -XDELETE').read()
        if(self.debug): print(self.res)

    def get_num_elements(self):
        if (self.debug) : print("get_num_elem")
        self.res = os.popen('curl -s http://'+self.server+':'+self.port+'/v2/keys/'+self.label+'/num_elem -XGET').read()
        if(self.debug): print(self.res)
        ans = json.loads(self.res)
        num = ans['node']['value']
        return int(num)

    def set_num_elements(self, num_elem):
        if (self.debug) : print("set_num_elem")
        self.res = os.popen('curl -s http://'+self.server+':'+self.port+'/v2/keys/'+self.label+'/num_elem -XPUT -d value='+str(num_elem)).read()
        if(self.debug): print(self.res)

    def get_lock(self):
        if (self.debug) : print("get_lock")
        #spin lock 
        while True:
            self.res = os.popen('curl -s http://'+self.server+':'+self.port+'/v2/keys/'+self.label+'/lock?prevValue=0 -XPUT -d value=1').read()
            if(self.debug): print(self.res)
            if 'errorCode' not in self.res:
                break
                
    def release_lock(self):
        if (self.debug) : print("release_lock")
        self.res = os.popen('curl -s http://'+self.server+':'+self.port+'/v2/keys/'+self.label+'/lock -XPUT -d value=0').read()
        if(self.debug): print(self.res)

    def push(s,value):
        if (s.debug) : print("push")
        s.get_lock()
        s.res = os.popen('curl -s http://'+s.server+':'+s.port+'/v2/keys/'+s.label+'/elements -XPOST -d value='+str(value)).read()
        if(s.debug): print(s.res)
        num_elem = s.get_num_elements();
        s.set_num_elements(num_elem + 1)
        s.release_lock()

    def pop(s):
        if (s.debug) : print("pop")
        s.get_lock()
        num_elem = s.get_num_elements();
        if num_elem == 0: return None
        s.res = os.popen('curl -s http://'+s.server+':'+s.port+'/v2/keys/'+s.label+'/elements').read()
        ans = json.loads(s.res)
        keys = ans['node']
        keys = keys['nodes']
        sorted_keys = keys.sort(key=lambda k: k['key'])
        value = keys[len(keys)-1]['value'] 
        key = keys[len(keys)-1]['key'] 
        s.res = os.popen('curl -s http://'+s.server+':'+s.port+'/v2/keys'+key+' -XDELETE').read()
        if(s.debug): print(s.res)
        s.set_num_elements(num_elem - 1)
        s.release_lock()
        return value
    
    def top(s):
        if (s.debug) : print("top")
        s.res = os.popen('curl -s http://'+s.server+':'+s.port+'/v2/keys/'+s.label+'/elements').read()
        ans = json.loads(s.res)
        keys = ans['node']
        keys = keys['nodes']
        sorted_keys = keys.sort(key=lambda k: k['key'])
        value = keys[len(keys)-1]['value'] 
        key = keys[len(keys)-1]['key'] 
        return value
     
    def qshow(s):
        res = os.popen('curl -s http://'+s.server+':'+s.port+'/v2/keys/'+s.label).read()
        print(res)
        res = os.popen('curl -s http://'+s.server+':'+s.port+'/v2/keys/'+s.label+'/elements').read()
        print(res)

# wrapper functions to provide specified interface
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
    s = queue_list[qid]
    if (s.debug): print("push:",s,s.label,x)
    s.push(x)

def pop(qid):
    s = queue_list[qid]
    if (s.debug): print("pop:",s,s.label)
    value = s.pop()
    return value

def get_qid(label):
    return queue_list_by_label[label].qid

def top(qid):
    s = queue_list[qid]
    value = s.top()
    return value

 
def qsize(qid):
    s = queue_list[qid]
    num_elem = s.get_num_elements();
    return num_elem

def qshow(qid):
    s = queue_list[qid]
    s.qshow()
    res = os.popen('curl -s http://'+s.server+':'+s.port+'/v2/keys/'+s.label).read()
    print(res)
    res = os.popen('curl -s http://'+s.server+':'+s.port+'/v2/keys/'+s.label+'/elements').read()
    print(res)




