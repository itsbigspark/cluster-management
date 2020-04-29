package com.bigspark.cloudera.management.helpers;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import javax.naming.ConfigurationException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ImpalaHelper {

  static Logger logger = LoggerFactory.getLogger(ImpalaHelper.class);

  private final String connectionString;
  private final Boolean isKerberos;

  //"jdbc:impala://dh-uwc-impala.server.rbsgrp.net:21051/default;AuthMech=3;transportMode=sasl"
  public ImpalaHelper(String connectionString, String userName, String password) {
    this.isKerberos = false;
    logger.debug(String
        .format("Constructed ImpalaHelper with LDAP Connection String: %s", connectionString));
    this.connectionString = String.format("%s;UID=%s;PWD=%s", connectionString, userName, password);
  }

  public ImpalaHelper(String connectionString) {
    this.connectionString = connectionString;
    this.isKerberos = true;
    logger.debug(String
        .format("Constructed ImpalaHelper with Connection String: %s", this.connectionString));
  }

  public void invalidateMetadata(String tableName)
      throws IllegalAccessException, InterruptedException, InstantiationException, IOException, SQLException, ClassNotFoundException {
    String[] parts_  = tableName.split("\\.");
    invalidateMetadata(parts_[0], parts_[1]);
  }

  public void invalidateMetadata(String dbName, String tableName)
      throws SQLException, InstantiationException, IllegalAccessException, ClassNotFoundException, IOException, InterruptedException {
    try (Connection conn = this.getConnection()) {
      invalidateMetadata(conn, dbName, tableName);
    }
  }

  public void computeStats(String dbName, String tableName, String partitionSpec)
      throws SQLException, InstantiationException, IllegalAccessException, ClassNotFoundException, IOException, InterruptedException {
    try (Connection conn = this.getConnection()) {
      try (Statement stmnt = conn.createStatement()) {
        stmnt.setQueryTimeout(600);
        stmnt.execute(String
            .format("refresh  %s.%s partition (%s)", dbName, tableName,
                partitionSpec));
        stmnt.execute(String
            .format("compute incremental stats %s.%s partition (%s)", dbName, tableName,
                partitionSpec));
      }
    }
  }

  public void invalidateMetadata(Connection conn, String dbName, String tableName)
      throws SQLException, InstantiationException, IllegalAccessException, ClassNotFoundException, IOException, InterruptedException {
    int attempt = 0;
    Boolean success = false;
    while (!success || attempt <= 3) {
      try {
        attempt++;
        try (Statement stmnt = conn.createStatement()) {
          stmnt.setQueryTimeout(600);
          stmnt.execute(String.format("invalidate metadata %s.%s", dbName, tableName));
          stmnt.execute(String.format("refresh %s.%s", dbName, tableName));
          success = true;
        }
      } catch (SQLException ex) {
        if (ex.getMessage()
            .contains("was modified while operation was in progress, aborting execution")) {
          Thread.sleep(1000);
        } else {
          throw ex;
        }
      }
    }
  }

  public void execute(String sql)
      throws SQLException, InstantiationException, IllegalAccessException, ClassNotFoundException, IOException, InterruptedException {
    try (Connection conn = this.getConnection()) {
      try (Statement stmnt = conn.createStatement()) {
        stmnt.setQueryTimeout(600);
        stmnt.execute(sql);
      }
    }
  }

  public Connection getConnection()
      throws IOException, InstantiationException, IllegalAccessException, ClassNotFoundException, InterruptedException, SQLException {
    UserGroupInformation ugi = UserGroupInformation.getLoginUser();
    final Driver driver = (Driver) Class.forName("com.cloudera.impala.jdbc41.Driver").newInstance();
    logger.trace(String.format("UGI: %s, keytab based: %s (Impala JDBC version: %d.%d)",
        ugi.toString()
        , ugi.isFromKeytab()
        , driver.getMajorVersion()
        , driver.getMinorVersion()));

    final String connectionString = this.connectionString;
    if(this.isKerberos) {
      return ugi.doAs(new PrivilegedExceptionAction<Connection>() {
        @Override
        public Connection run() throws Exception {
          return driver.connect(connectionString, new Properties());
        }
      });
    } else {
      return driver.connect(connectionString, new Properties());
    }
  }

  public static ImpalaHelper getInstanceFromProperties(Properties  jobProperties) throws Exception {

      if (jobProperties.containsKey("impala.connStr.LDAP")) {
        try {
          logger.debug("Attempting to construct LDAP Impala Helper");
          String connStr = jobProperties.getProperty("impala.connStr.LDAP");
          String userName = SparkHelper.getSparkUser();
          String keyStore = String.format("%s/keystore.jceks", SparkHelper.Hdfs.getUserHomeArea());
          String keyName  = String.format("%s.pwd", userName);
          if(FileSystemHelper.getConnection().exists(new Path(keyStore))) {
            logger.trace(String.format("Getting password for key %s from %s", keyName, keyStore));
            String password = SparkHelper.Security.getFromHadoopKeyStore(
                keyStore
                , keyName
            );
            return new ImpalaHelper(connStr, userName, password);
          } else {
            throw new ConfigurationException("Keystore in format /user/<racf>/keystore.jceks does not exist");
          }
        } catch(Exception ex) {
          throw new Exception ("Could not initialise LDAP Impala, check exception", ex);
        }
      } else {
        logger.debug("Attempting to construct KRB5 Impala Helper");
        String connStr = jobProperties.getProperty("impala.connStr");
        return new ImpalaHelper(connStr);
      }
  }
}
