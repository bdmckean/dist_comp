#!/usr/bin/env python3

import subprocess
import sys
import os
import json
import FTqueue



# Test driver 
qid = FTqueue.create_Queue('rt')

FTqueue.push(qid,5)
FTqueue.push(qid,6)
FTqueue.push(qid,9)
FTqueue.push(qid,3)
FTqueue.qshow(qid)
print(FTqueue.qsize(qid))

x = 1000
x = FTqueue.pop(qid)

print(x)
FTqueue.qshow(qid)

FTqueue.del_Queue(qid)




