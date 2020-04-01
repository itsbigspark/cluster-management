#/bin/bash

export HOME_DIR="/Users/${USER}/workspace"
export HADOOP_CONF_DIR="/opt/hadoop/hadoop-conf/do"
export SPARK_HOME="/opt/spark/latest"

key="${myKey}"
secret="${mySecret}"
bucket="${myBucket}"

$SPARK_HOME/bin/spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --conf spark.yarn.archive=hdfs:///user/bigspark/spark-libs-cdh.jar \
     --class com.bigspark.cloudera.management.jobs.offload.OffloadRunner \
    "file:///${HOME_DIR}/cluster-management/target/cluster-management-1.0-SNAPSHOT.jar" \
    "fs.s3a.access.key=$key" \
    "fs.s3a.secret.key=$secret" \
    "src=/user" \
    "tgt=s3a://${bucket}/distcp-test-do/"


#--conf spark.yarn.archive=hdfs:///user/cloudera/spark-libs.jar \
