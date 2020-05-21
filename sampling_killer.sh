#!/bin/bash

TIMEOUT=$1




echo "Sleeping $TIMEOUT"

sleep $TIMEOUT


PID=`jps | grep SparkSubmit | awk '{print $1}'`

if [ ! -f "sampling_done.txt" ]; then

echo "Killing $PID after $1 seconds..."
kill $PID

touch killed.txt

fi

