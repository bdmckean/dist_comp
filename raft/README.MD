# FTQueue
##Project for distributed computing class
Implements a queue on top of the etcd raft cluster

## Resources
### etcd
See these web sites for how to start a etcd raft cluster
https://github.com/coreos/etcd

### Goreman
Goreman is used to start the etcd across 5 processes on the local machien
this could also be used to spawn machines remotely
Procfile inclused the startup information for goreman that starts 5 processes
https://github.com/mattn/goreman

## Project code
https://github.com/co-bri/dist_comp/tree/master/raft

Uses keys on the raft server to store queue elements
Keeps count of elements in raft data base
Uses raft key for a loking element

### steps for running
1) Download and install goreman & etcd
2) Paths must be set correctly.  "source set_path.sh" will add the local "go/bin" directory to the path
3) start etcd "gorman start"
4) run "test.py" to excercise the queue

### Known limitaions
1) Not much error checking
2) only one client test using test.py -- need multiple clients interacting to fully test lock interactions. 
3) uses hard coded server in test.py, need means to find surviving servers in case this server fails
4) Hangs if more raft quorum has too few servers
5) Must manually kill and restart servers
6) Locking is spin lock with no timeouts

## License MIT
