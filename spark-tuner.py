#!/usr/bin/python

import sys
import json
import csv

job_name = sys.argv[1]
job_input = sys.argv[2]
job_eff = sys.argv[3]

suggested_num_execs = None;
suggested_vcores_per_exec = None;
suggested_vmem_per_exec = None;

# goal: f(job_name, job_input (data size), job_effeciency) = (execs, vcores, vmem)
# 3. (lookup current resource availablity from the yarn cluster)
# 6. (scale up or down based upon current resource availability)
# 7. return winners
# bonus: use break-away points in scaling effort

historical_data = []
with open("submit-history.log", "rb") as f:
	reader = csv.reader(f, delimiter=",")
	next(reader) # skip header
	for line in reader:
		historical_data.append(line)

# filter out those results from a different job
historical_data = [trial for trial in historical_data if trial[0] == job_name]

# TODO: filter out results with a different input size (this is tricky because in some jobs, eg iterative ones, the parameters to the algorithm have more of an influence over the data size) 

# sort from highest -> lowest runtime
historical_data = sorted(historical_data, key=lambda l:l[5], reverse=True)

# for trial in historical_data:
# 	print str(trial)

# TODO: map to efficiency distribution. play with various distribution types.

# TODO: do magic here
suggested_num_execs = 2;
suggested_vcores_per_exec = 2;
suggested_vmem_per_exec = "4g";

suggestions = {"execs": suggested_num_execs, "vcores": suggested_vcores_per_exec, "vmem": suggested_vmem_per_exec};

# print tuning parameters as a single JSON object to stdout
print(json.dumps(suggestions))
