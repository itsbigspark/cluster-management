# EDH - ClusterManagement
###Compilation 

`mvn package`

Will output 2 jars:  Main artifact and -tests.jar

###Testing

Can test in local mode using embedded derby db
Should be workable in deploy-mode cluster with appropriate $HADOOP_CONF_DIR set


##### purging

`export SPARK_SUBMIT_OPTS=-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5050 && $SPARK_HOME/bin/spark-submit --master local --class dev.bigspark.cloudera.management.services.purging.PurgingJobTestRunner --jars ~/edh-cluster-management/target/cluster-management-1.0-SNAPSHOT.jar  ~/edh-cluster-management/target/cluster-management-1.0-SNAPSHOT-tests.jar
`

######Entry point - PurgingJobTestRunner 
o Process will check for test data at /tmp/testdata.csv, if not existing, it will generate it

o Process will check for existence of tables:

__default.test_table_sh__
__default.test_table_eas__
__default.data_retention_configuration__

* database is configurable in config.properties


