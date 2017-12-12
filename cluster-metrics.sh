#!/bin/bash

# TODO: make port / path configurable
curl --silent ukko166.hpc.cs.helsinki.fi:8088/ws/v1/cluster/metrics | python -m "json.tool"
