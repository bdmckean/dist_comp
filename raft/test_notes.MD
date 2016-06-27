# Test plan for queue using raft

## Overall test process

1 set up 5 server raft instance and proxy
2 set up 1 test drivers - run test.py
3 kill 2 servers (manually)
4 verify that queue information can be retireved
5 verify all queue functions - run test.py
6 kill 3rd server
7 verify queue does not work - run test.py

## These are the test files
test.py -- tests that one queue drver works

## API for queue

### These APIs require a lock
void push (int queue_id, int item); 
    // enters item in the queue
int pop (int queue_id); 
    // removes an item from the queue and returns it 

### These APIs do not require a lock
int create_Queue (int label); 
    //Creates a new queue of integers; associates this 
    //queue with label and returns a queue id (int)
int get_qid (int label); 
    //returns queue id of the queue associated with label 
int top (int queue_id); 
    // returns the value of the first element in the queue 
int qsize (int queue_id); 
    // returns the number of items in the queue 

FTQueue must be able to tolerate up to two server crash failures 

