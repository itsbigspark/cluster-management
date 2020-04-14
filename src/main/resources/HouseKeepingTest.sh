#!/usr/bin/env bash

#Assumes correct environment is set e.g. switch dev
export PROJECT_DIR="/export/home/$USER/workspace/cluster-management"
export PROJECT_JAR="$PROJECT_DIR/target/cluster-management-1.0-SNAPSHOT.jar"
echo "SPARK_HOME=$SPARK_HOME"
echo "HADOOP_CONF_DIR=$HADOOP_CONF_DIR"
echo "KEYTAB=$KEYTAB"
echo "PRINCIPAL=$PRINCIPAL"
echo "KRB5_CONFIG=$KRB5_CONFIG"
echo "PROJECT_DIR=$PROJECT_DIR"
echo "PROJECT_JAR=$PROJECT_JAR"
unset SPARK_CONF_DIR

$SPARK_HOME/bin/spark-submit \
            --master yarn \
            --deploy-mode client \
            --keytab "${KEYTAB}" \
            --principal "${PRINCIPAL}" \
            --jars hdfs:///user/oozie/libext/sqoop_jdbc/ImpalaJDBC41.jar \
            --conf spark.driver.extraJavaOptions="-Dlog4j.configuration=file://${PROJECT_DIR}/src/main/resources/log4j.properties -Djava.security.krb5.conf=$KRB5_CONFIG" \
            --class com.bigspark.cloudera.management.jobs.purging.PurgingRunner \
            $PROJECT_JAR

