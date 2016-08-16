#!/bin/bash

# start monitor an all machines
# -- these monitors will collect data on all machines
# run this script on machine with HiBench installed
# input is which test to run

# this sctipt will del all log files at teh start
# montiroes will continue writing 

#!/bin/bash



machines=(
    172.31.23.33
    172.31.27.36
    172.31.27.34
    172.31.27.33
    172.31.27.37
    172.31.27.35
    )

for i in "${machines[@]}"
do
    ssh root@$i "yum -y install sar"

done
