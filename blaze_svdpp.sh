TEST_NAME=SVDPP-iter5-synth1tb
TEST_TYPE=Spark
EXECUTORS=10

FRACTION=0.5
DAG_PATH=None

CLASS=org.apache.spark.examples.graphx.SVDPlusPlusExample
JAR=/home/ubuntu/spark/examples/target/scala-2.11/jars/spark-examples_2.11-2.4.4.jar
ARGS=dummy
MEM_OVERHEAD=6g

SLACK=0
PROMOTE=0.0
PROFILE_TIMEOUT=60

MEM_SIZE=340g
DISAGG=0g
./drdd_runner.sh $TEST_NAME $TEST_TYPE $MEM_SIZE $EXECUTORS $FRACTION $DISAGG $DAG_PATH $CLASS $JAR $MEM_OVERHEAD $SLACK $PROMOTE $PROFILE_TIMEOUT $ARGS 



















