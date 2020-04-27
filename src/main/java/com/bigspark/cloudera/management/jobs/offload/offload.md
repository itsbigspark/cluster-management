# bigspark - Cloudera Management suite
## Offload component
###Purpose

The offload component transfers data from HDFS/Hive to Object storage by policy based configuration and manages metadata migration.

##Behaviour

Entry Point 1
Class Name: com.bigspark.cloudera.management.jobs.offload.OffloadRunner

##Arguments:

Group=x - if not present all records from the configuration are returned

##Config Properties:

This file should be located in hdfs://user/<racf>/cluster-management/config.properties of the executing user:

`sparkAppNamePrefix=EDH CLUSTER MANAGEMENT :`

`isDryRun=false`

`offload.metatable=bdmsysmm01p.sys_cm_offload`

`offload.auditTable=bdmsysmm01p.sys_cm_offload_audit`

`offload.sqlAuditTable=bdmsysmm01p.sys_cm_spark_sql_audit`

`impala.connStr=jdbc:impala://dh-uwc-impala.server.rbsgrp.net:21050;AuthMech=1;KrbRealm=EUROPA.RBSGRP.NET;KrbHostFQDN=dh-uwc-impala.server.rbsgrp.net;KrbServiceName=service-impala-p`


##Logging

General log4j/slf4j logging - Is driven by a log4j properties file. One has been included that will minimise “noisy” spark logging to stdout and focus on Application logging

Spark SQL Auditing - All spark SQL operations are logged to com.bigspark.cloudera.management.services.spark.auditTable

Job Auditing - Used for automated recovery by BAU - Logged to offload.auditTable


##Integration testing
`export HADOOP_CONF_DIR=/Users/chris/hadoop/t20/hive-conf && \
 ./bin/spark-submit \
 --master yarn \
 --deploy-mode cluster \
 --conf spark.yarn.archive="hdfs:///user/chris/spark-libs-cds240r2.jar" \
 --class com.bigspark.cloudera.management.jobs.offload.OffloadJobTestRunner \
 --jars ~/edh-cluster-management/target/cluster-management-1.0-SNAPSHOT.jar \
 ~/edh-cluster-management/target/cluster-management-1.0-SNAPSHOT-tests.jar`

####Entry point - OffloadJobTestRunner 
Process will check for test data at ${USERHOME}/testdata.csv, if not existing, it will generate it.   
Process will check for existence of tables:

`default.test_table_sh` *   
`default.test_table_eas` *   
`default.offload_configuration` *   

* database.table is configurable in `tst/resources/config.properties`

General Notes
Offload policies are explicitly applied at DB and Table in a Hive configuration table

The offload service will move data to a trash location in the executing users home directory rather than using a hard delete

The trash location will be cleared on a user configurable time based policy
