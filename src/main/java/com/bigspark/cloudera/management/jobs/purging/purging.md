# bigspark - Cloudera Management suite
# Purging component
###Purpose
The Purging component  safely removes data from the cluster driven by policy based configuration.

###General Notes
Purge policies are explicitly applied at DB and Table in a Hive configuration table

The purging service will move data to a trash location rather than using a hard delete

The trash location will be cleared on a user configurable time based policy

##Behaviour

Entry Point 1
Class Name: com.bigspark.cloudera.management.jobs.housekeeping.HousekeepingRunner

##Arguments:

housekeepingGroup=x - if not present all records from the confirguration are returned

##Config Properties:

This file should be located in hdfs://user/<racf>/cluster-management/config.properties of the executing user:

`sparkAppNamePrefix=EDH CLUSTER MANAGEMENT :`

`isDryRun=false`

`purging.metatable=bdmsysmm01p.sys_cm_purging`

`purging.auditTable=bdmsysmm01p.sys_cm_purging_audit`

`purging.sqlAuditTable=bdmsysmm01p.sys_cm_spark_sql_audit`

`impala.connStr=jdbc:impala://dh-uwc-impala.server.rbsgrp.net:21050;AuthMech=1;KrbRealm=EUROPA.RBSGRP.NET;KrbHostFQDN=dh-uwc-impala.server.rbsgrp.net;KrbServiceName=service-impala-p`


##Logging

General log4j/slf4j logging - Is driven by a log4j properties file.  One has been included that will minimise “noisy” spark logging to stdout and focus on Application logging

Spark SQL Auditing - All spark SQL operations are logged to com.bigspark.cloudera.management.services.spark.auditTable

Job Auditing - Used for automated recovery by BAU -  Logged to purging.auditTable

##Integration testing
`export SPARK_SUBMIT_OPTS=-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5050 \
&& $SPARK_HOME/bin/spark-submit \
--master yarn \
--deploy-mode cluster \
--class com.bigspark.cloudera.management.services.purging.PurgingJobTestRunner \
--jars ~/edh-cluster-management/target/cluster-management-1.0-SNAPSHOT.jar  \
~/edh-cluster-management/target/cluster-management-1.0-SNAPSHOT-tests.jar`

###Entry point - PurgingJobTestRunner
Process will check for test data at ${USERHOME}/testdata.csv, if not existing, it will generate it.   
Process will check for existence of tables:

`default.test_table_sh` *   
`default.test_table_eas` *   
`default.purging_configuration` *   

* database.table is configurable in `tst/resources/config.properties`
##
