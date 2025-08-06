#!/bin/bash

# Start Spark master
start-master.sh -p 7077

# Start Spark worker
start-worker.sh spark://spark-lance:7077

# Start History Server
start-history-server.sh

# Start Thrift Server
start-thriftserver.sh --driver-java-options "-Dderby.system.home=/tmp/derby"

# Entrypoint, for example notebook, pyspark or spark-sql
if [[ $# -gt 0 ]] ; then
    eval "$1"
fi