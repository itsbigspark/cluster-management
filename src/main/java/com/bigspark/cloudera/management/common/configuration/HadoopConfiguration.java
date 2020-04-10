package com.bigspark.cloudera.management.common.configuration;

import java.io.IOException;
import javax.naming.ConfigurationException;
import org.apache.hadoop.security.UserGroupInformation;

public class HadoopConfiguration {

  String keytab() throws ConfigurationException {
    if (System.getenv("KEYTAB") != null) {
      return System.getenv("KEYTAB");
    } else {
      throw new ConfigurationException("KEYTAB  environment variable not provided");
    }
  }


  String principal() throws ConfigurationException {
    if (System.getenv("PRINCIPAL") != null) {
      return System.getenv("PRINCIPAL");
    } else {
      throw new ConfigurationException("PRINCIPAL environment variable not provided");
    }
  }

  public UserGroupInformation userGroupInformation() throws IOException, ConfigurationException {
    String principal;
    String keytabFile;
    if (System.getenv("KEYTAB") == null || System.getenv("PRINCIPAL") == null) {
      throw new ConfigurationException("KEYTAB or PRINCIPAL environment variables not provided");
    }
    keytabFile = keytab();
    principal = principal();
    try {
      return UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, keytabFile);
    } catch (IOException e) {
      e.printStackTrace();
      throw e;
    }
  }
}
