#!/usr/bin/python

import sys
import json
import csv

job_name = sys.argv[1]
job_input = sys.argv[2]
job_eff = int(sys.argv[3])

# print "Job Name: " + job_name
# print "Job Input: " + job_input
# print "Job Efficiency: " + str(job_eff)

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

# using runtimes from the historical data, create a linear interpolation which maps
# {0, 1, 2, 3, 4, 5, 6, 7, 8, 9} --> {<fastest runtime>, ..., <middle runtime>, ..., <slowest runtime>
# If y_0 is the fastest (lowest time) and y_n is the slowest (highest) runtime, then
# f(efficiency) = y_0 + efficiency*(y_n - y_0)/9
# this returns an approximate runtime to target for, and we search for the nearest data point for that runtime
def map_eff_linear(efficiency, historical_data):
	y_0 = int(historical_data[-1][5])
	y_n = int(historical_data[0][5])

	result = int(y_0 + (efficiency/9.0) * (y_n - y_0))
	
	nearest_runtime = None
	
	# search historical_data for runtime most-close to result,
	for i, data_point in enumerate(historical_data):
		next_runtime = int(data_point[5])

		if i == 0 and next_runtime == result:	
			nearest_runtime = (data_point[2], data_point[3], data_point[4])
		else:
			prev_runtime = int(historical_data[i-1][5])

			if (prev_runtime >= result) and (next_runtime <= result):
				# found where it belongs
				nearest_runtime = (data_point[2], data_point[3], data_point[4])
	
	return nearest_runtime	

# for testing all the output values
# for i in range(10):
#	print str(i) + " --> " + str(map_eff_linear(i, historical_data))

(suggested_num_execs, suggested_vcores_per_exec, suggested_vmem_per_exec) = map_eff_linear(job_eff, historical_data)

suggestions = {"execs": suggested_num_execs, "vcores": suggested_vcores_per_exec, "vmem": suggested_vmem_per_exec};

# print tuning parameters as a single JSON object to stdout
print(json.dumps(suggestions))
