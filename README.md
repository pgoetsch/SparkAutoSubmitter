# SparkAutoSubmitter

## Usage
```
bash spark-submit-wrapper.sh <job-type> <input-data> <cluster-efficiency>
```
Where:
* `job-type` is an identifier for the job, eg "ScalaWordCount"
* `input-data` is an HDFS path where the input data to the job lives
* `cluster-efficiency` is an integer [0-9] with 0=most emphasis on the performance of the single job and 9=performance of the computing cluster as a whole

## Installation

1. Clone the repository in the same environment that has the prerequisite software listed below.
2. Update spark-environment.conf with the values for your system

### Prerequisites / Supported Runtime
Software | Version
------------ | ---------------
Apache Spark | 2.1 (with hadoop 2.7)
Hadoop | 2.8.1

## Open Ends

* [ ] read and write results to hdfs path
* [ ] make more things configurable
* [ ] figure out how to support multiple job types (and also those not in hibench)
