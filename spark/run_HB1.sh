#!/bin/bash

# start monitor an all machines
# -- these monitors will collect data on all machines
# run this script on machine with HiBench installed
# input is which test to run

# this sctipt will del all log files at teh start
# montiroes will continue writing 

#!/bin/bash

for i in "$@"
do
case $i in
    -t=*|--test=*)
    TEST="${i#*=}"
    shift # past argument=value
    ;;
    -r=*|--run=*)
    RUN="${i#*=}"
    shift # past argument=value
    ;;
    -x=*|--x=*)
    TYPE="${i#*=}"
    shift # past argument=value
    ;;
    *)
           # unknown option
    ;;
esac
done

OUTFILE="HB$TEST-$RUN.result"
START="/root/mon/mon.sh"
GATHER="cat /root/mon/*.log > /root/mon/$OUTFILE"
KILL="/root/mon/killsar.sh"

echo $TYPE

if [ "$TYPE" == "mr" ]
then
    TESTPATH="/root/HiBench/workloads/$TEST/mapreduce/bin/run.sh"
else
    TESTPATH="/root/HiBench/workloads/$TEST/spark/java/bin/run.sh"
fi


machines=(
    172.31.23.33
    172.31.27.36
    172.31.27.34
    172.31.27.33
    172.31.27.37
    172.31.27.35
    )

#echo $TEST 
#echo $START 
#echo $GATHER 
#echo $KILL
#echo $TESTPATH

#Prep for test
echo "Preparing data"
time workloads/$TEST/prepare/prepare.sh

#start monitor on all machines
echo "stating monitor on all machines"
for i in "${machines[@]}"
do
    ssh root@$i "rm -f /root/mon/*.log"
    ssh root@$i $START
done

sleep 20
# Run Test
echo "Run test"
TIMEFILE="HB$TEST-$TYPE-$RUN.time"
echo "run $TESTPATH"
{ time $TESTPATH ;} 2>./results/$TIMEFILE

sleep 5

for i in "${machines[@]}"
do
    OUTFILE="HB$TEST-$i-$TYPE-$RUN.result"
    GATHER="cat /root/mon/*.log > /root/mon/$OUTFILE"
    ssh root@$i "$KILL"
    ssh root@$i "$GATHER"
    scp root@$i:/root/mon/$OUTFILE ./results

done

ssh root@${machines[0]}  "/root/ephemeral-hdfs/bin/hadoop fs -rmr /HiBench"
