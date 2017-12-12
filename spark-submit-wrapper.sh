#!/bin/bash

if [ -z "$1" ] || [ -z "$2" ] || [ -z "$3" ]; then
	echo "Usage: spark-submit-v2.sh <job-type> <input-data> <cluster-efficiency>"
	echo "  job-type: Job type identifier"
	echo "  input-data: Path to HDFS input data"
	echo "  cluster-efficiency: An integer [0-9] with 0=most emphasis on the performance of the single job and 9=performance of the computing cluster as a whole"
	exit;
fi

job_type=$1
input_data=$2
cluster_eff=$3

# load configuration
source spark-environment.conf

# call to python script for calculating the tuning parameters
tuning_results=$(python spark-tuner.py $job_type $input_data $cluster_eff)

echo $tuning_results
# TODO: from tuning_results, parse-out suggested_*
suggested_execs=4
suggested_vcores=4
suggested_vmem="10g"

# job-specific preparation
export SPARKBENCH_PROPERTIES_FILES=/cs/work/home/goetsch/dev/HiBench-master/report/wordcount/spark/conf/sparkbench/sparkbench.conf

# clean output
$hadoop_home/bin/hadoop --config $hadoop_home/etc/hadoop fs -rm -r -skipTrash hdfs://ukko166.hpc.cs.helsinki.fi:56000/HiBench/Wordcount/Output

# call spark-submit
$spark_home/bin/spark-submit --class com.intel.hibench.sparkbench.micro.ScalaWordCount --master yarn-client --num-executors $suggested_execs --executor-cores $suggested_vcores --executor-memory $suggested_vmem /cs/work/home/goetsch/dev/HiBench-master/sparkbench/assembly/target/sparkbench-assembly-6.1-SNAPSHOT-dist.jar hdfs://ukko166.hpc.cs.helsinki.fi:56000/HiBench/Wordcount/Input hdfs://ukko166.hpc.cs.helsinki.fi:56000/HiBench/Wordcount/Output &> spark-submit.log
# TODO: optimize-out the intermediate file

# grab app ID from logs
app_id=$(grep -m 1 "Application report for application_[0-9_]* (state: ACCEPTED)" spark-submit.log | cut -d' ' -f8)
echo "Application ID found: $app_id"

# sleep briefly to allow the spark history server to log everything
sleep 15s

# retrieve spark data about app from spark history server
app_data=$(curl --silent "ukko166.hpc.cs.helsinki.fi:18080/api/v1/applications/$app_id");
echo "Job details from spark: $app_data"

# extract duration (ms) of job from json response
duration=$(echo "$app_data" | grep "duration" | sed 's/[^0-9]*//g');
echo "Job Duration Parsed: $duration ms"

# TODO: eventually write this to hdfs instead
record=$(echo "$job_type,SIZETODO,$suggested_execs,$suggested_vcores,$suggested_vmem,$duration");

# append details of this job to history log
echo $record >> submit-history.log
