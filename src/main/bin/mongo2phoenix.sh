#!/bin/bash
source /etc/profile
source ~/.bashrc

cd `dirname $0`
BIN_DIR=`pwd`
cd ..
DEPLOY_DIR=`pwd`
BASE_DIR=${DEPLOY_DIR}
echo $BASE_DIR
CONF_DIR=$BASE_DIR/conf
echo $CONF_DIR
LIB_DIR=$BASE_DIR/lib
LIB_JARS=`ls $LIB_DIR|grep .jar|awk '{print "'$LIB_DIR'/"$0}'|tr "\n" ","`

spark-submit --class cn.wangz.spark.job.Mongo2PhoenixJob  \
    --master yarn \
    --deploy-mode cluster \
    --driver-memory 2g \
    --executor-memory 4g \
    --num-executors  2 \
    --executor-cores 1 \
    --driver-cores 1 \
    --conf "spark.default.parallelism=50" \
    --conf "spark.sql.shuffle.partitions=100" \
    --conf "spark.yarn.executor.memoryOverhead=1024" \
    --conf "spark.executor.extraJavaOptions=-XX:+UseG1GC" \
    --conf "spark.port.maxRetries=100" \
    --conf "spark.dynamicAllocation.enabled=false" \
    --conf "spark.ui.enabled=true" \
    --jars $LIB_JARS \
    --files $SPARK_HOME/conf/hive-site.xml,$SPARK_HOME/conf/hbase-site.xml \
    $BASE_DIR/lib/SparkDemo-1.0-SNAPSHOT.jar