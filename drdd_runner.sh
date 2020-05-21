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

ARGS="${@:14}"

branch=`git rev-parse --abbrev-ref HEAD`
my_branch="spl-master"
my_branch="tg-2nd-draft-debug"
#my_branch="tg-2nd-draft-refactoring"
#my_branch="tg-2.4-local-disagg-debug"

echo "current branch $branch"

if [ "$TEST_TYPE" == "Spark" ] && [ "$branch" != "vanilla-v2.4.4" ]; then
	echo "building branch $branch"	
	git checkout vanilla-v2.4.4
	./build/sbt -Phadoop-2.7,yarn -DskipTests package		
fi

if [ "$TEST_TYPE" == "Spark-Disk" ] && [ "$branch" != "vanilla-v2.4.4-memdisk" ]; then
	echo "building branch vanilla-v2.4.4-memdisk"	
	git checkout vanilla-v2.4.4-memdisk
	./build/sbt -Phadoop-2.7,yarn -DskipTests package		

fi

if [ "$TEST_TYPE" == "Blaze-Reverse" ] && [ "$branch" != "tg-2nd-draft-disagg" ]; then
	echo "building branch tg-2nd-draft-disagg"	
	git checkout tg-2nd-draft-disagg 
	./build/sbt -Phadoop-2.7,yarn -DskipTests package		

elif [[ $TEST_TYPE != *"Spark"* ]] && [ "$branch" != "$my_branch" ]; then
	echo "building branch $branch"	
	git checkout $my_branch
	./build/sbt -Phadoop-2.7,yarn -DskipTests package		
fi


PROFILING=true
CACHING_POLICY=Blaze
MEMORY_MANAGER=Unified
AUTOCACHING=true
DISAGG_ONLY=false
DISAGG_FIRST=false

if [[ $TEST_TYPE == *"Spark"* ]]; then
AUTOCACHING=false
PROFILING=false

elif [ "$TEST_TYPE" == "Blaze-Disagg" ]; then
MEMORY_MANAGER=Disagg
DISAGG_ONLY=true
COST_FUNCTION=Blaze-MRD
EVICT_POLICY=Cost-size-ratio2

elif [ "$TEST_TYPE" == "Blaze-Both" ]; then
echo "both"
COST_FUNCTION=Blaze-MRD
EVICT_POLICY=Cost-size-ratio2

elif [ "$TEST_TYPE" == "Blaze-Stage-Ref" ]; then

COST_FUNCTION=Blaze-Stage-Ref
EVICT_POLICY=Cost-size-ratio2


elif [ "$TEST_TYPE" == "Blaze-Reverse" ]; then

COST_FUNCTION=Blaze-MRD
EVICT_POLICY=Cost-size-ratio2
DISAGG_FIRST=true

elif [ "$TEST_TYPE" == "Blaze-No-profile-autocaching" ]; then
COST_FUNCTION=Blaze-Stage-Ref
EVICT_POLICY=Cost-size-ratio2
AUTOCACHING=true
PROFILING=false
DAG_PATH=None

elif [ "$TEST_TYPE" == "Blaze-No-profile" ]; then
COST_FUNCTION=Blaze-Stage-Ref
EVICT_POLICY=Cost-size-ratio2
AUTOCACHING=false
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


elif [ "$TEST_TYPE" == "Blaze-Cost-based" ]; then
COST_FUNCTION=Blaze-MRD
EVICT_POLICY=Cost-based

elif [ "$TEST_TYPE" == "Blaze-Explicit" ]; then

COST_FUNCTION=Blaze-MRD
EVICT_POLICY=Cost-size-ratio2
AUTOCACHING=false

elif [ "$TEST_TYPE" == "Real-LRC" ]; then

COST_FUNCTION=LRC
EVICT_POLICY=Cost-based

AUTOCACHING=false
PROFILING=false
DAG_PATH=None

elif [ "$TEST_TYPE" == "Real-LRC-profile" ]; then

COST_FUNCTION=LRC
EVICT_POLICY=Cost-based

AUTOCACHING=false
PROFILING=true

elif [ "$TEST_TYPE" == "Blaze-MRD-profile" ]; then

COST_FUNCTION=MRD
EVICT_POLICY=Cost-based
AUTOCACHING=false
PROFILING=true

elif [ "$TEST_TYPE" == "Blaze-MRD" ]; then

COST_FUNCTION=MRD
EVICT_POLICY=Cost-based
AUTOCACHING=false
PROFILING=false
DAG_PATH=None

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


if [ "$TEST_TYPE" != "Spark" ]; then
parallel-ssh -h ~/crail-hosts.txt 'sudo rm -rf /dev/hugepages/cache/*'
parallel-ssh -h ~/crail-hosts.txt 'sudo rm -rf /dev/hugepages/data/*'

parallel-ssh -h ~/compute-hosts.txt 'sudo rm -rf /dev/hugepages/cache/*'
parallel-ssh -h ~/compute-hosts.txt 'sudo rm -rf /dev/hugepages/data/*' 

parallel-ssh -h ~/compute-hosts.txt 'sudo rm -rf /home/ubuntu/xvdc/yarn/*'


# recompile crail 

stop-crail.sh && start-crail.sh
sleep 5
fi


CORES=14
NODES=10
EXECUTOR_PER_NODE=$(( EXECUTORS / $NODES))
CORES=$(( CORES / (EXECUTOR_PER_NODE+1) + 1 ))
#CORES=8
echo $CORES

NAME=a
NUM=1
DATE=`date +"%m-%d"`

echo "DATE $DATE"
DIR=logs/$DATE/$1/$TEST_TYPE-mem$MEM_SIZE-executors$EXECUTORS-fraction$FRACTION-disagg$DISAGG_THRESHOLD-autocaching$AUTOCACHING-profiling$PROFILING-cost$COST_FUNCTION-evict$EVICT_POLICY-slack$SLACK-mOverhead-$MEM_OVERHEAD-promote$PROMOTE-$NUM

while [ -d "$DIR" ]
do
  # Control will enter here if $DIRECTORY exists.
  NUM=$(( NUM + 1 ))
  DIR=logs/$DATE/$1/$TEST_TYPE-mem$MEM_SIZE-executors$EXECUTORS-fraction$FRACTION-disagg$DISAGG_THRESHOLD-autocaching$AUTOCACHING-profiling$PROFILING-cost$COST_FUNCTION-evict$EVICT_POLICY-slack$SLACK-mOverhead-$MEM_OVERHEAD-promote$PROMOTE-$NUM

done


echo "ARGUMENT $ARGS"
echo "JAR $JAR"
echo "CLASS $CLASS"


rm sampled_log.txt
rm sampled_lineage.txt
rm spark_log.txt
rm completed.txt


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



if [[ $TEST_NAME == *"LGR"* ]]; then

./bin/spark-submit -v \
--packages com.microsoft.ml.spark:mmlspark_2.11:1.0.0-rc1 \
--num-executors $EXECUTORS --executor-cores $CORES --executor-memory $MEM_SIZE \
--master yarn --class $CLASS \
--conf "spark.yarn.am.memory=4000m" \
--conf "spark.yarn.am.cores=2" \
--conf "spark.driver.extraClassPath=/home/ubuntu/.m2/repository/com/google/guava/guava/14.0.1/guava-14.0.1.jar:$CRAIL_JAR/*:." \
--conf "spark.driver.memory=32000m" \
--conf "spark.driver.cores=6" \
--conf "spark.executor.extraClassPath=/home/ubuntu/.m2/repository/com/google/guava/guava/14.0.1/guava-14.0.1.jar:$CRAIL_JAR/*:." \
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
--conf "spark.driver.extraClassPath=/home/ubuntu/.m2/repository/com/google/guava/guava/14.0.1/guava-14.0.1.jar:$CRAIL_JAR/*:." \
--conf "spark.driver.memory=32000m" \
--conf "spark.driver.cores=6" \
--conf "spark.executor.extraClassPath=/home/ubuntu/.m2/repository/com/google/guava/guava/14.0.1/guava-14.0.1.jar:$CRAIL_JAR/*:." \
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
hdfs dfs -get /spark_history/$APP_ID sampled_lineage.txt


if [ ! -f "sampled_lineage.txt" ]; then
	hdfs dfs -get /spark_history/"${APP_ID}.inprogress" sampled_lineage.txt
fi

wait


DAG_PATH=sampled_lineage.txt
#rm $DIR/sampled_log.txt

sleep 2

if [ -f "killed.txt" ]; then
FULLY=false
else
FULLY=true
fi

fi

mv blaze.log sampled_blaze.log
rm blaze.log



echo "FULLY $FULLY"

echo "Actual Execution!!"

if [ "$TEST_TYPE" != "Spark-Disk" ]; then
echo "Start exception observer and killer.."
python exception_killer.py &
fi


start_time="$(date -u +%s)"


if [[ $TEST_NAME == *"LGR"* ]]; then
./bin/spark-submit -v \
	--packages com.microsoft.ml.spark:mmlspark_2.11:1.0.0-rc1 \
	--num-executors $EXECUTORS --executor-cores $CORES --executor-memory $MEM_SIZE \
	--master yarn --class $CLASS \
	--conf "spark.yarn.am.memory=6000m" \
	--conf "spark.yarn.am.cores=2" \
	--conf "spark.driver.extraClassPath=/home/ubuntu/.m2/repository/com/google/guava/guava/14.0.1/guava-14.0.1.jar:$CRAIL_JAR/*:." \
	--conf "spark.driver.memory=48000m" \
	--conf "spark.driver.cores=8" \
	--conf "spark.executor.extraClassPath=/home/ubuntu/.m2/repository/com/google/guava/guava/14.0.1/guava-14.0.1.jar:$CRAIL_JAR/*:." \
	--conf "spark.rpc.lookupTimeout=300s" \
	--conf "spark.memory.memoryManager=$MEMORY_MANAGER" \
	--conf "spark.disagg.cachingpolicy=$CACHING_POLICY" \
	--conf "spark.disagg.threshold=$DISAGG_THRESHOLD" \
	--conf "spark.memory.fraction=0.6" \
	--conf "spark.memory.storageFraction=$FRACTION" \
	--conf "spark.disagg.dagpath=$DAG_PATH" \
	--conf "spark.disagg.disableLocalCaching=$DISAGG_ONLY" \
	--conf "spark.disagg.costfunction=$COST_FUNCTION" \
	--conf "spark.disagg.evictionpolicy=$EVICT_POLICY" \
	--conf "spark.disagg.autocaching=$AUTOCACHING" \
	--conf "spark.disagg.sampledRun=false" \
	--conf "spark.disagg.memoryslack=$SLACK" \
	--conf "spark.disagg.first=$DISAGG_FIRST" \
	--conf "spark.disagg.promote=$PROMOTE" \
	--conf "spark.disagg.fullyProfiled=$FULLY" \
	--conf "spark.executor.memoryOverhead=$MEM_OVERHEAD" \
	--conf "spark.rpc.netty.dispatcher.numThreads=22" \
	--conf "spark.driver.maxResultSize=2g" \
	$JAR $ARGS \
	2>&1 | tee spark_log.txt

#--conf "spark.memory.fraction=$FRACTION" \
else
./bin/spark-submit -v \
	--num-executors $EXECUTORS --executor-cores $CORES --executor-memory $MEM_SIZE \
	--master yarn --class $CLASS \
	--conf "spark.yarn.am.memory=6000m" \
	--conf "spark.yarn.am.cores=2" \
	--conf "spark.driver.extraClassPath=/home/ubuntu/.m2/repository/com/google/guava/guava/14.0.1/guava-14.0.1.jar:$CRAIL_JAR/*:." \
	--conf "spark.driver.memory=48000m" \
	--conf "spark.driver.cores=8" \
	--conf "spark.executor.extraClassPath=/home/ubuntu/.m2/repository/com/google/guava/guava/14.0.1/guava-14.0.1.jar:$CRAIL_JAR/*:." \
	--conf "spark.rpc.lookupTimeout=300s" \
	--conf "spark.memory.memoryManager=$MEMORY_MANAGER" \
	--conf "spark.disagg.cachingpolicy=$CACHING_POLICY" \
	--conf "spark.disagg.threshold=$DISAGG_THRESHOLD" \
	--conf "spark.memory.fraction=0.6" \
	--conf "spark.memory.storageFraction=$FRACTION" \
	--conf "spark.disagg.dagpath=$DAG_PATH" \
	--conf "spark.disagg.disableLocalCaching=$DISAGG_ONLY" \
	--conf "spark.disagg.costfunction=$COST_FUNCTION" \
	--conf "spark.disagg.evictionpolicy=$EVICT_POLICY" \
	--conf "spark.disagg.autocaching=$AUTOCACHING" \
	--conf "spark.disagg.sampledRun=false" \
	--conf "spark.disagg.memoryslack=$SLACK" \
	--conf "spark.disagg.first=$DISAGG_FIRST" \
	--conf "spark.disagg.fullyProfiled=$FULLY" \
	--conf "spark.disagg.promote=$PROMOTE" \
	--conf "spark.executor.memoryOverhead=$MEM_OVERHEAD" \
	--conf "spark.rpc.netty.dispatcher.numThreads=22" \
	$JAR $ARGS \
	2>&1 | tee spark_log.txt
fi

echo "Creating $DIR"
mkdir -p $DIR

mv sampled_lineage.txt $DIR/
mv sampled_log.txt $DIR/
mv sampled_blaze.log $DIR/
mv spark_log.txt $DIR/log.txt

touch completed.txt

end_time="$(date -u +%s)"
elapsed="$(($end_time-$start_time))"
sampling_time="$(($sampling_end-$sampling_start))"


# wait exception killer
wait


# extract app id
APP_ID=`cat $DIR/log.txt | grep -oP "application_[0-9]*_[0-9]*" | tail -n 1`
~/get_log.sh $APP_ID
mv log_$APP_ID $DIR/executor_logs.txt

EXCEPTION=`cat $DIR/log.txt | grep Exception | head -10`
CANCEL=`cat $DIR/log.txt | grep cancelled as part of`

if [ -z "${EXCEPTION// }" ]; then
echo "haha"
else
	message="Exception happended!!!!! $EXCEPTION\n"
fi

if [ -z "${CANCEL// }" ]; then
echo "hoho"
else
	message="Job cancelled!!!!!\n"
fi

message=$message"App $APP_ID\n"
message=$message"Local memory $MEM_SIZE\n"
message=$message"Profiling timeout $PROFILE_TIMEOUT\n"
message=$message"Disagg memory $DISAGG_THRESHOLD\n"
message=$message"Time $elapsed seconds\n"
message=$message"Sampling $sampling_time seconds\n"
message=$message"Executors $EXECUTORS\n"
message=$message"Log $DIR\n"
message=$message"Args $ARGS\n"
message=$message"--conf spark.disagg.promote=$PROMOTE\n"
message=$message"--conf spark.memory.memoryManager=$MEMORY_MANAGER\n"
message=$message"--conf spark.disagg.cachingpolicy=$CACHING_POLICY\n"
message=$message"--conf spark.disagg.threshold=$DISAGG_THRESHOLD\n"
message=$message"--conf spark.disagg.costfunction=$COST_FUNCTION\n"
message=$message"--conf spark.disagg.evictionpolicy=$EVICT_POLICY\n"
message=$message"--conf spark.disagg.autocaching=$AUTOCACHING\n"

# history parsing 
hdfs dfs -get /spark_history/$APP_ID $DIR/history.txt
python3 parsing_history.py $DIR/history.txt > $DIR/total_gc_time.txt

GCTIME=`cat $DIR/total_gc_time.txt`

message=$message"GC time: $GCTIME\n"

send_slack.sh  $message

mv blaze.log $DIR/

# disagg size 
cat $DIR/log.txt |  grep -oP "Disagg total size: [0-9]* \(MB\)" > $DIR/disagg_mem.txt


# mem size
cat $DIR/log.txt | grep "Total size" | python3 memory_use_parser.py > $DIR/time_mem_use.txt

# recomp time
cat $DIR/log.txt | grep "RCTime" | python3 recomp_parser.py > $DIR/recomp.txt

# evict tcost
cat $DIR/log.txt | grep "EVICT" | python3 evict_parser.py > $DIR/evict.txt
