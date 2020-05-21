#!/bin/bash

echo "Killing Spark"


PID=`jps | grep SparkSubmit | awk '{print $1}'`

echo "Killing $PID"
kill $PID

