# Test plan for queue using raft

## Overall test process

1 set up 5 server raft instance and proxy
2 set up 2 test drivers
3 set up driver to kill two servers while 2 running
4 end 
5 verify that queue information can be retireved
6 verify all queue fucntions
7 kill 3rd server
8 verify queue does not work

## These are the test files
queue_test.py -- sets up the queue handlers and uses them to test the queue
queue_unit.py -- tests that one queue works
queue.py -- sets up a queue handler with a target server 

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

