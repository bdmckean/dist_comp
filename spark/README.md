# Code for Spark analysis of stackexhnage data

## Load spark on aws using 
https://github.com/amplab/spark-ec2

(Note that this also sets up hadoop under spark -- and looks likt the easiest way to set up a hadoop cluster in AW)

### spin up spark cluster
export AWS_SECRET_ACCESS_KEY=AaBbCcDdEeFGgHhIiJjKkLlMmNnOoPpQqRrSsTtU
export AWS_ACCESS_KEY_ID=ABCDEFG1234567890123
./spark-ec2 --key-pair=awskey --identity-file=awskey.pem --region=us-west-1 --zone=us-west-1a launch my-spark-cluster

###
- ssh into server and upload data
- Load data into ephemeral_hdfs 
- upload your job
- Then do your job with spark_submit



files:
count.py : counts lines which include the name of a programming language
proces_posts.py : takes the original data, puts it into a dictionary, converts all letters to lower case and the write to a new file only the data in the "nody" record.  The output firl of the is the input to count.py


process_posts.py was run on my client machine
count.py was run in spark and those are the results included in my homework


The file wc.py was also run on the spark cluster and it was attemot to do a staring count of words across all of the stackexchange data.  I discarded these results as not being what i was lookig for and created the count.py file to repalce it
