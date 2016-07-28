#!/usr/bin/env python3


source = open("posts.xml", "r")

test_count = 0
count = 1
dest = open('post_body.txt', "w")
while(True):

    test_count += 1
    #if test_count > 5:
    #    break

    line = source.readline()
    #print (line)
    if len(line) == 0:
        break
    
    if "row Id" not in line:
        continue
    line.replace('<','')
    line.replace('/>','')
    d = {}
    fields = (line.split('" '))
    for entry in fields:
        #print (entry)
        if '=' not in entry:
            continue
        key, value = map(str, entry.split('="'))
        # strip quotes from string
        #print(key,value)
        d[key] = value
        #print(d[key]) 
        if key == 'Body':
            dest.write(value.lower()+'\n')
    #print (fields[0])
    #print (d)
    # Partition record into files incrememns of 10000 ID's

    

dest.close()
source.close()
