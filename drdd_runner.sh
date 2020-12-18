#!/bin/bash
#unused var
AUTOSIZING_COMP=0.5

TEST_NAME=$1 # ALSExample
TEST_TYPE=$2
MEM_SIZE=$3 # 24g
EXECUTORS=$4
FRACTION=$5
DISAGG_THRESHOLD=$6 
DAG_PATH=$7 # None: no dag path, we need sampling
CLASS=$8 # PR: com.ibm.crail.benchmarks.Main
JAR=$9
MEM_OVERHEAD=${10}
SLACK=${11}
PROMOTE=${12}
PROFILE_TIMEOUT=${13}
MEM_FRACTION=${14}
DISK_THRESHOLD=${15}

ARGS="${@:16}"

branch=`git rev-parse --abbrev-ref HEAD`

echo "current branch $branch"

parallel-ssh -i -h /home/ubuntu/compute-hosts.txt 'rm -rf /home/ubuntu/spark_cache/*'	

echo "Remove spark shuffle dir"
parallel-ssh -i -h /home/ubuntu/compute-hosts.txt 'rm -rf /home/ubuntu/spark_shuffle/*'	


echo "Stopping iostats ..."
parallel-ssh -h /home/ubuntu/compute-hosts.txt pkill iostat


PROFILING=true
CACHING_POLICY=Blaze
MEMORY_MANAGER=Unified
AUTOCACHING=true
DISAGG_ONLY=false
DISAGG_FIRST=false

USE_DISK=false
LOC_UNAWARE=false

DISABLE_LOCAL=false 
ZIGZAG_RATIO=0.5

IS_SPARK=false
CACHING_UNCONDITIONAL=false

if [[ $TEST_TYPE == "Spark" ]]; then
COST_FUNCTION=No
EVICT_POLICY=Cost-based
AUTOCACHING=false
PROFILING=false
DAG_PATH=None
USE_DISK=true
IS_SPARK=true
CACHING_UNCONDITIONAL=false

elif  [[ $TEST_TYPE == "Spark-Autocaching" ]]; then
COST_FUNCTION=No
EVICT_POLICY=Cost-based
AUTOCACHING=true
PROFILING=true
DAG_PATH=None
USE_DISK=true
IS_SPARK=true

elif [ "$TEST_TYPE" == "Blaze-Caching-Unconditional" ]; then

COST_FUNCTION=Blaze-Disk-Recomp
EVICT_POLICY=Cost-based
USE_DISK=true
CACHING_UNCONDITIONAL=true

elif [ "$TEST_TYPE" == "Blaze-Disk-Recomp-Cost" ]; then

COST_FUNCTION=Blaze-Disk-Recomp
EVICT_POLICY=Cost-based
USE_DISK=true

elif [ "$TEST_TYPE" == "Blaze-Disk-Recomp-Cost-No-Zigzag" ]; then

COST_FUNCTION=Blaze-Disk-Recomp
EVICT_POLICY=Cost-based
USE_DISK=true
ZIGZAG_RATIO=1

elif [ "$TEST_TYPE" == "Blaze-Disk-Future-Use" ]; then

COST_FUNCTION=Blaze-Disk-Future-Use
EVICT_POLICY=Cost-based
USE_DISK=true


elif [ "$TEST_TYPE" == "Blaze-Disk-Cost" ]; then

COST_FUNCTION=Blaze-Disk-Only
EVICT_POLICY=Cost-based
USE_DISK=true

elif [ "$TEST_TYPE" == "Blaze-Recomp-Cost" ]; then

COST_FUNCTION=Blaze-Recomp-Only
EVICT_POLICY=Cost-based
USE_DISK=true


elif [ "$TEST_TYPE" == "Blaze-No-Profiling" ]; then
COST_FUNCTION=Blaze-Disk-Recomp
EVICT_POLICY=Cost-based
USE_DISK=true
AUTOCACHING=true
PROFILING=false
DAG_PATH=None

elif [ "$TEST_TYPE" == "Blaze-Linear-Dist" ]; then

COST_FUNCTION=Blaze-Linear-Dist
EVICT_POLICY=Cost-size-ratio2
AUTOCACHING=true
PROFILING=true

elif [ "$TEST_TYPE" == "Blaze-Time-Only" ]; then

COST_FUNCTION=Blaze-Time-Only
EVICT_POLICY=Cost-based
AUTOCACHING=true
PROFILING=true


elif [ "$TEST_TYPE" == "Blaze-Ref-Only" ]; then

COST_FUNCTION=Blaze-Ref-Only
EVICT_POLICY=Cost-based
AUTOCACHING=true
PROFILING=true

elif [ "$TEST_TYPE" == "Blaze-Leaf-Cnt" ]; then

COST_FUNCTION=Blaze-Leaf-Cnt
EVICT_POLICY=Cost-based
AUTOCACHING=true
PROFILING=true

elif [ "$TEST_TYPE" == "Blaze-Ref-Cnt-RDD" ]; then

COST_FUNCTION=Blaze-Ref-Cnt
EVICT_POLICY=RDD-Ordering
AUTOCACHING=true
PROFILING=true

elif [ "$TEST_TYPE" == "Blaze-Explicit" ]; then

COST_FUNCTION=Blaze-Stage-Ref
EVICT_POLICY=Cost-size-ratio2
AUTOCACHING=false

elif [ "$TEST_TYPE" == "LCS" ]; then

COST_FUNCTION=LCS
EVICT_POLICY=Cost-based
AUTOCACHING=false
PROFILING=false
DAG_PATH=None
USE_DISK=true


elif [ "$TEST_TYPE" == "LRC" ]; then

COST_FUNCTION=LRC
EVICT_POLICY=Cost-based
AUTOCACHING=false
PROFILING=false
DAG_PATH=None
USE_DISK=true

elif [ "$TEST_TYPE" == "LRC-Disk" ]; then

COST_FUNCTION=LRC
EVICT_POLICY=Cost-based
AUTOCACHING=false
PROFILING=false
DAG_PATH=None
USE_DISK=true


elif [ "$TEST_TYPE" == "LRC-profile" ]; then

COST_FUNCTION=LRC
EVICT_POLICY=Cost-based
AUTOCACHING=false
PROFILING=true

elif [ "$TEST_TYPE" == "MRD" ]; then

COST_FUNCTION=MRD
EVICT_POLICY=Cost-based
AUTOCACHING=false
PROFILING=false
DAG_PATH=None
USE_DISK=true

elif [ "$TEST_TYPE" == "MRD-Disk" ]; then

COST_FUNCTION=MRD
EVICT_POLICY=Cost-based
AUTOCACHING=false
PROFILING=false
DAG_PATH=None
USE_DISK=true


else

echo "No $TEST_TYPE!!!!!!!!!!!!!1"
exit 125

fi

#MEMORY_MANAGER=$2 # Unified / Static / Disagg (all)
#STORING_POLICY=$3 # Default (storing evicted blocks into disagg) / No (do not store)
#CACHING_POLICY=$4 # None (no caching after getting blocks from disagg) / Local (caching)
#EVICT_POLICY=${10}
#AUTOCACHING=${11}
#AUTOSIZING_COMP=${12}

ITER=3


parallel-ssh -h ~/crail-hosts.txt 'sudo rm -rf /dev/hugepages/cache/*'
parallel-ssh -h ~/crail-hosts.txt 'sudo rm -rf /dev/hugepages/data/*'

parallel-ssh -h ~/compute-hosts.txt 'sudo rm -rf /dev/hugepages/cache/*'
parallel-ssh -h ~/compute-hosts.txt 'sudo rm -rf /dev/hugepages/data/*' 

parallel-ssh -h ~/compute-hosts.txt 'sudo rm -rf /home/ubuntu/spark_cache/*'
parallel-ssh -h ~/compute-hosts.txt 'sudo rm -rf /home/ubuntu/spark_cache/data/*'

stop-crail.sh

#parallel-ssh -h ~/compute-hosts.txt 'sudo rm -rf /home/ubuntu/xvdc/yarn/*'


# recompile crail 

if [[ $TEST_TYPE == *"Blaze"* ]]; then
start-crail.sh
sleep 5
fi


CORES=14
NODES=10
EXECUTOR_PER_NODE=$(( EXECUTORS / $NODES))
CORES=$(( CORES / (EXECUTOR_PER_NODE + EXECUTOR_PER_NODE) + 1 ))
#CORES=2
echo $CORES

NAME=a
NUM=1
DATE=`date +"%m-%d"`

echo "DATE $DATE"
timestamp=$(date +%s)

DIR=logs/$DATE/$1/$TEST_TYPE-mem$MEM_SIZE-disk$DISK_THRESHOLD-executors$EXECUTORS-fraction$FRACTION-memfrac-$MEM_FRACTION-disagg$DISAGG_THRESHOLD-autocaching$AUTOCACHING-profiling$PROFILING-cost$COST_FUNCTION-evict$EVICT_POLICY-slack$SLACK-mOverhead-$MEM_OVERHEAD-promote$PROMOTE-core$CORES-$timestamp-$NUM

while [ -d "$DIR" ]
do
  # Control will enter here if $DIRECTORY exists.
  NUM=$(( NUM + 1 ))
DIR=logs/$DATE/$1/$TEST_TYPE-mem$MEM_SIZE-disk$DISK_THRESHOLD-executors$EXECUTORS-fraction$FRACTION-memfrac-$MEM_FRACTION-disagg$DISAGG_THRESHOLD-autocaching$AUTOCACHING-profiling$PROFILING-cost$COST_FUNCTION-evict$EVICT_POLICY-slack$SLACK-mOverhead-$MEM_OVERHEAD-promote$PROMOTE-core$CORES-$timestamp-$NUM

done


echo "ARGUMENT $ARGS"
echo "JAR $JAR"
echo "CLASS $CLASS"


rm sampled_log.txt
rm sampled_lineage.txt
rm spark_log.txt
rm completed.txt


EXTRA_PATH=/home/ubuntu/.m2/repository/com/google/guava/guava/14.0.1/guava-14.0.1.jar:/home/ubuntu/hadoop/share/hadoop/common/lib/*:$CRAIL_JAR/*

sampling_start="$(date -u +%s)"
FULLY=false

if [ "$DAG_PATH" == "None" ] && [ "$PROFILING" == "true" ]; then
	echo "Sampling!!"
	true

rm blaze.log
rm sampling_done.txt
rm killed.txt 

SAMPLING_TIME=$PROFILE_TIMEOUT
sampling_killer.sh $SAMPLING_TIME &

#--conf "spark.driver.extraClassPath=/home/ubuntu/.m2/repository/com/google/guava/guava/14.0.1/guava-14.0.1.jar:$CRAIL_JAR/*:/home/ubuntu/hadoop/share/hadoop/common/lib/hadoop-aws-2.7.2.jar:/home/ubuntu/hadoop/share/hadoop/common/lib/aws-java-sdk-1.7.4.jar" \



if [[ $TEST_NAME == *"LGR"* ]]; then

./bin/spark-submit -v \
--packages com.microsoft.ml.spark:mmlspark_2.11:1.0.0-rc1 \
--num-executors $EXECUTORS --executor-cores $CORES --executor-memory $MEM_SIZE \
--master yarn --class $CLASS \
--conf "spark.yarn.am.memory=4000m" \
--conf "spark.yarn.am.cores=2" \
--conf "spark.driver.extraClassPath=$EXTRA_PATH" \
--conf "spark.driver.memory=32000m" \
--conf "spark.driver.cores=6" \
--conf "spark.executor.extraClassPath=$EXTRA_PATH" \
--conf "spark.rpc.lookupTimeout=300s" \
--conf "spark.memory.memoryManager=Unified" \
--conf "spark.disagg.threshold=$DISAGG_THRESHOLD" \
--conf "spark.storage.memoryFraction=0.6" \
--conf "spark.disagg.costfunction=No" \
--conf "spark.disagg.dagpath=$DAG_PATH" \
--conf "spark.disagg.autocaching=false" \
--conf "spark.disagg.sampledRun=true" \
--conf "spark.driver.maxResultSize=2g" \
$JAR $ARGS \
2>&1 | tee sampled_log.txt
else

./bin/spark-submit -v \
--num-executors $EXECUTORS --executor-cores $CORES --executor-memory $MEM_SIZE \
--master yarn --class $CLASS \
--conf "spark.yarn.am.memory=4000m" \
--conf "spark.yarn.am.cores=2" \
--conf "spark.driver.extraClassPath=$EXTRA_PATH" \
--conf "spark.driver.memory=32000m" \
--conf "spark.driver.cores=6" \
--conf "spark.executor.extraClassPath=$EXTRA_PATH" \
--conf "spark.rpc.lookupTimeout=300s" \
--conf "spark.memory.memoryManager=Unified" \
--conf "spark.disagg.threshold=$DISAGG_THRESHOLD" \
--conf "spark.storage.memoryFraction=0.6" \
--conf "spark.disagg.dagpath=$DAG_PATH" \
--conf "spark.disagg.costfunction=No" \
--conf "spark.disagg.autocaching=false" \
--conf "spark.disagg.sampledRun=true" \
$JAR $ARGS \
2>&1 | tee sampled_log.txt
fi

touch sampling_done.txt

sampling_end="$(date -u +%s)"

# get dag path 
APP_ID=`cat sampled_log.txt | grep -oP "application_[0-9]*_[0-9]*" | tail -n 1`
echo "Getting sampled lineage... $APP_ID"
sleep 5
hdfs dfs -get /spark_history/$APP_ID sampled_lineage.txt


if [ ! -f "sampled_lineage.txt" ]; then
	hdfs dfs -get /spark_history/"${APP_ID}.inprogress" sampled_lineage.txt
fi

#echo "Waiting.."
#wait
#echo "Wait done"


DAG_PATH=sampled_lineage.txt
#rm $DIR/sampled_log.txt

sleep 5

if [ -f "killed.txt" ]; then
FULLY=false
else
FULLY=true
fi

fi

mv blaze.log sampled_blaze.log
rm blaze.log
rm disk_log.txt

parallel-ssh -i -h $HOME/compute-hosts.txt 'rm -rf /home/ubuntu/spark_cache/*'	

echo "Remove spark shuffle dir"
parallel-ssh -i -h $HOME/compute-hosts.txt 'rm -rf /home/ubuntu/spark_shuffle/*'	


echo "FULLY $FULLY"

echo "Actual Execution!!"

echo "Start exception observer and killer.."
python exception_killer.py &


start_time="$(date -u +%s)"

echo "Start iostat"
parallel-ssh -h ../compute-hosts.txt "nohup ./run_iostat.sh > /dev/null 2>&1 &"

sleep 5

LOCALITY_WAIT=3s

if [[ $TEST_NAME == *"LGR"* ]]; then
./bin/spark-submit -v \
	--packages com.microsoft.ml.spark:mmlspark_2.11:1.0.0-rc1 \
	--num-executors $EXECUTORS --executor-cores $CORES --executor-memory $MEM_SIZE \
	--master yarn --class $CLASS \
	--conf "spark.yarn.am.memory=4000m" \
	--conf "spark.yarn.am.cores=2" \
	--conf "spark.driver.extraClassPath=$EXTRA_PATH" \
	--conf "spark.driver.memory=48000m" \
	--conf "spark.driver.cores=8" \
	--conf "spark.executor.extraClassPath=$EXTRA_PATH" \
	--conf "spark.rpc.lookupTimeout=300s" \
	--conf "spark.memory.memoryManager=$MEMORY_MANAGER" \
	--conf "spark.disagg.cachingpolicy=$CACHING_POLICY" \
	--conf "spark.disagg.threshold=$DISAGG_THRESHOLD" \
	--conf "spark.memory.fraction=$MEM_FRACTION" \
	--conf "spark.memory.storageFraction=$FRACTION" \
	--conf "spark.disagg.dagpath=$DAG_PATH" \
	--conf "spark.disagg.disableLocalCaching=$DISABLE_LOCAL" \
	--conf "spark.disagg.costfunction=$COST_FUNCTION" \
	--conf "spark.disagg.evictionpolicy=$EVICT_POLICY" \
	--conf "spark.disagg.autocaching=$AUTOCACHING" \
	--conf "spark.locality.wait=$LOCALITY_WAIT" \
	--conf "spark.disagg.sampledRun=false" \
	--conf "spark.disagg.memoryslack=$SLACK" \
	--conf "spark.disagg.first=$DISAGG_FIRST" \
	--conf "spark.disagg.promote=$PROMOTE" \
	--conf "spark.disagg.fullyProfiled=$FULLY" \
	--conf "spark.executor.memoryOverhead=$MEM_OVERHEAD" \
	--conf "spark.rpc.netty.dispatcher.numThreads=160" \
	--conf "spark.driver.maxResultSize=2g" \
	--conf "spark.storage.threshold=$DISAGG_THRESHOLD" \
	--conf "spark.disagg.useLocalDisk=$USE_DISK" \
	--conf "spark.disagg.zigzag=$ZIGZAG_RATIO" \
	--conf "spark.disagg.isspark=$IS_SPARK" \
	--conf "spark.disagg.disk.threshold=$DISK_THRESHOLD" \
	--conf "spark.disagg.diskLocalityUnaware=$LOC_UNAWARE" \
	--conf "spark.disagg.cachingUnconditionally=$CACHING_UNCONDITIONAL" \
	$JAR $ARGS \
	2>&1 | tee spark_log.txt

#--conf "spark.memory.fraction=$FRACTION" \
else
./bin/spark-submit -v \
	--num-executors $EXECUTORS --executor-cores $CORES --executor-memory $MEM_SIZE \
	--master yarn --class $CLASS \
	--conf "spark.yarn.am.memory=4000m" \
	--conf "spark.yarn.am.cores=2" \
	--conf "spark.driver.extraClassPath=$EXTRA_PATH" \
	--conf "spark.driver.memory=50000m" \
	--conf "spark.driver.cores=8" \
	--conf "spark.executor.extraClassPath=$EXTRA_PATH" \
	--conf "spark.locality.wait=$LOCALITY_WAIT" \
	--conf "spark.rpc.lookupTimeout=300s" \
	--conf "spark.rpc.askTimeout=600s" \
	--conf "spark.memory.memoryManager=$MEMORY_MANAGER" \
	--conf "spark.disagg.cachingpolicy=$CACHING_POLICY" \
	--conf "spark.disagg.threshold=$DISAGG_THRESHOLD" \
	--conf "spark.memory.fraction=$MEM_FRACTION" \
	--conf "spark.memory.storageFraction=$FRACTION" \
	--conf "spark.disagg.dagpath=$DAG_PATH" \
	--conf "spark.disagg.disableLocalCaching=$DISABLE_LOCAL" \
	--conf "spark.disagg.costfunction=$COST_FUNCTION" \
	--conf "spark.disagg.evictionpolicy=$EVICT_POLICY" \
	--conf "spark.disagg.autocaching=$AUTOCACHING" \
	--conf "spark.disagg.sampledRun=false" \
	--conf "spark.disagg.memoryslack=$SLACK" \
	--conf "spark.disagg.first=$DISAGG_FIRST" \
	--conf "spark.disagg.fullyProfiled=$FULLY" \
	--conf "spark.disagg.promote=$PROMOTE" \
	--conf "spark.executor.memoryOverhead=$MEM_OVERHEAD" \
	--conf "spark.rpc.netty.dispatcher.numThreads=160" \
	--conf "spark.storage.threshold=$DISAGG_THRESHOLD" \
	--conf "spark.disagg.useLocalDisk=$USE_DISK" \
	--conf "spark.disagg.zigzag=$ZIGZAG_RATIO" \
	--conf "spark.disagg.isspark=$IS_SPARK" \
	--conf "spark.disagg.disk.threshold=$DISK_THRESHOLD" \
	--conf "spark.disagg.diskLocalityUnaware=$LOC_UNAWARE" \
	--conf "spark.disagg.cachingUnconditionally=$CACHING_UNCONDITIONAL" \
	$JAR $ARGS \
	2>&1 | tee spark_log.txt
fi

echo "Creating $DIR"
mkdir -p $DIR

echo "Stopping iostats ..."
parallel-ssh -h /home/ubuntu/compute-hosts.txt pkill iostat

sleep 1

for i in {1..10}
do
scp w$i:/home/ubuntu/iostat_log.txt $DIR/iostat_log_w$i.txt
python3 disk_time_range.py $DIR/iostat_log_w$i.txt > $DIR/parsed_iostat_w$i.txt
rm $DIR/iostat_log_w$i.txt
done

mv sampled_lineage.txt $DIR/
mv sampled_log.txt $DIR/
mv sampled_blaze.log $DIR/
mv spark_log.txt $DIR/log.txt

touch completed.txt

end_time="$(date -u +%s)"
elapsed="$(($end_time-$start_time))"
sampling_time="$(($sampling_end-$sampling_start))"


# wait exception killer and disk logger
wait

if [ -f "disk_log.txt" ]; then
mv disk_log.txt $DIR/
fi


# extract app id
APP_ID=`cat $DIR/log.txt | grep -oP "application_[0-9]*_[0-9]*" | tail -n 1`

EXCEPTION=`cat $DIR/log.txt | grep Exception | head -3`
CANCEL=`cat $DIR/log.txt | grep cancelled because | head -3`
ABORT=`cat $DIR/log.txt | grep aborted | head -3`

if [ -z "${EXCEPTION// }" ]; then
echo "haha"
else
	message="Exception happended!!!!! $EXCEPTION\n"
fi

if [ -z "${CANCEL// }" ]; then
echo "hoho"
else
	message="Job cancelled!!!!! $CANCEL\n"
fi

if [ -z "${ABORT// }" ]; then
echo "hihi"
else
	message="Job aborted!!!!! $ABORT\n"

fi

COMMIT=`git log --pretty=format:'%h' -n 1`
BRANCH=`git rev-parse --abbrev-ref HEAD`

message=$message"Commit $COMMIT\n"
message=$message"Branch $BRANCH\n"
message=$message"App $APP_ID\n"
message=$message"Local memory $MEM_SIZE\n"
message=$message"Profiling timeout $PROFILE_TIMEOUT\n"
message=$message"Disk $DISK_THRESHOLD\n"
message=$message"Disagg memory $DISAGG_THRESHOLD\n"
message=$message"Time $elapsed seconds\n"
message=$message"Sampling $sampling_time seconds\n"
message=$message"Executors $EXECUTORS\n"
message=$message"$DIR\n"
message=$message"Args $ARGS\n"
message=$message"--conf spark.locality.wait=$LOCALITY_WAIT\n" 
message=$message"--conf spark.disagg.promote=$PROMOTE\n"
message=$message"--conf spark.memory.memoryManager=$MEMORY_MANAGER\n"
message=$message"--conf spark.disagg.cachingpolicy=$CACHING_POLICY\n"
message=$message"--conf spark.disagg.threshold=$DISAGG_THRESHOLD\n"
message=$message"--conf spark.disagg.costfunction=$COST_FUNCTION\n"
message=$message"--conf spark.disagg.evictionpolicy=$EVICT_POLICY\n"
message=$message"--conf spark.disagg.autocaching=$AUTOCACHING\n"

./send_slack.sh  $message

sleep 20

~/get_log.sh $APP_ID
mv log_$APP_ID $DIR/executor_logs.txt
touch $DIR/commit-$COMMIT
touch $DIR/branch-$BRANCH
touch $DIR/appid-$APP_ID

# history parsing 
if [[ ${#APP_ID} -gt 5 ]]; then
	hdfs dfs -get /spark_history/$APP_ID $DIR/history.txt
fi

GCTIME=`cat $DIR/total_gc_time.txt`

message=$message"GC time: $GCTIME\n"


mv blaze.log $DIR/

# rsync
echo "rsyncing..."
rsync -rzavh -e  'ssh -p 2222' logs/ jyeo@147.46.216.122:/home/jyeo/atc21/tg-m/logs


# disagg size 
cat $DIR/log.txt |  grep -oP "Disagg total size: [0-9]* \(MB\)" > $DIR/disagg_mem.txt


# mem size
cat $DIR/log.txt | grep "Total size" | python3 memory_use_parser.py > $DIR/time_mem_use.txt

# recomp time
cat $DIR/log.txt | grep "RCTime" | python3 recomp_parser.py > $DIR/recomp.txt

# evict tcost
cat $DIR/log.txt | grep "EVICT" | python3 evict_parser.py > $DIR/evict.txt
