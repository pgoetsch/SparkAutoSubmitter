#!/usr/bin/python

import sys
import json

job_name = sys.argv[1]
job_input = sys.argv[2]
job_eff = sys.argv[3]

suggested_num_execs = None;
suggested_vcores_per_exec = None;
suggested_vmem_per_exec = None;

# TODO: do magic here
suggested_num_execs = 2;
suggested_vcores_per_exec = 2;
suggested_vmem_per_exec = "4g";

suggestions = {"execs": suggested_num_execs, "vcores": suggested_vcores_per_exec, "vmem": suggested_vmem_per_exec};

# print tuning parameters as a single JSON object to stdout
print(json.dumps(suggestions))
