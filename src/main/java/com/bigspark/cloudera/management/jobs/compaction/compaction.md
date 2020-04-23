# bigspark - Cloudera Management suite
## Compaction component
###Purpose
The compaction component reads HDFS partitions and compacts small parquet files into optimal sizes by policy based configuration.


##Integration testing

##Entry point - CompactionJobTestRunner
`export HADOOP_CONF_DIR=/Users/chris/hadoop/t20/hive-conf \
&& export SPARK_SUBMIT_OPTS=-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5050 \
&& ./bin/spark-submit \
--master yarn \
--deploy-mode cluster \
--class com.bigspark.cloudera.management.jobs.compaction.CompactionJobTestRunner \
--jars ~/edh-cluster-management/target/cluster-management-1.0-SNAPSHOT.jar \
~/edh-cluster-management/target/cluster-management-1.0-SNAPSHOT-tests.jar`
