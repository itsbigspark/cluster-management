package com.bigspark.cloudera.management.common.utils;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
import java.util.Date;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.alias.CredentialProvider;
import org.apache.hadoop.security.alias.CredentialProvider.CredentialEntry;
import org.apache.hadoop.security.alias.CredentialProviderFactory;

//import org.apache.hadoop.fs.FileSystem;

@Deprecated
public class Utils {

  private static boolean logDebugLevel = false;

  // private static final Log LOGGER = LogFactory.getLog(Utils.class);
  private static final Log LOGGER = LogFactory.getLog(Utils.class);

  public static String dumpProperties(final Properties properties) {
    return PropertyUtils.dumpProperties(properties);
  }

  public static CredentialEntry getCredential(String storePath, String alias) throws IOException {
    Configuration conf = new Configuration();
    conf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH, storePath);
    CredentialProvider provider = CredentialProviderFactory.getProviders(conf).get(0);
    CredentialEntry ce = provider.getCredentialEntry(alias);
    return ce;
  }

  public static Date getDate(Date currDate, int daysToSubtract) {
    return DateUtils.getDate(currDate, daysToSubtract);
  }

  public static Date getDate(String date) throws ParseException {
    return DateUtils.getDate(date);
  }

  public static Date getDate(String date, String dateFormat) throws ParseException {
    return DateUtils.getDate(date, dateFormat);
  }

  public static int getDay(Date currDate) {
    return DateUtils.getDay(currDate);
  }

  public static int getDayOfWeek(Date currDate) {
    return DateUtils.getDayOfWeek(currDate);
  }

  public static Date getEDIFileDate(FileStatus currFile) {
    return DateUtils.getEDIFileDate(currFile);
  }

  public static Date getEDIFileDate(String fileName) throws ParseException {
    return DateUtils.getEDIFileDate(fileName);
  }

  public static String getFile(final String fileSpec) throws IOException {
    return org.apache.commons.io.IOUtils.toString(openFile(fileSpec));
  }

  public static String getFormattedDate(Date currDate) throws ParseException {
    return DateUtils.getFormattedDate(currDate);
  }

  public static String getFormattedDate(Date currDate, String dateFormat) throws ParseException {
    return DateUtils.getFormattedDate(currDate, dateFormat);
  }

  public static int getMonth(Date currDate) {
    return DateUtils.getMonth(currDate);
  }

  public static String getOptionalProperty(Properties properties, String key, String defaultValue) {
    return PropertyUtils.getOptionalProperty(properties, key, defaultValue);
  }

  // @SuppressWarnings("unchecked")
  @SuppressWarnings("unchecked")
  public static <T> T getOptionalProperty(Properties properties, String key, T defaultValue) {
    return PropertyUtils.getOptionalProperty(properties, key, defaultValue);
  }

  public static Properties getProgramArgsAsProps(String[] args) {
    return PropertyUtils.getProgramArgsAsProps(args);
  }

  public static Properties getPropertiesFile(InputStream props) throws IOException {
    return PropertyUtils.getPropertiesFile(props);
  }

  public static Properties getPropertiesFile(String propertiesFileSpec) throws IOException {
    return PropertyUtils.getPropertiesFile(propertiesFileSpec);
  }

  public static String getPropertyValue(Properties props, String key, String defaultValue) {
    return PropertyUtils.getPropertyValue(props, key, defaultValue);
  }

  public static int getYear(Date currDate) {
    return DateUtils.getYear(currDate);
  }

  public static boolean isLogDebugLevel() {
    return logDebugLevel;
  }

  public static InputStream openFile(final String fileSpec) throws IOException {
    if (fileSpec.startsWith("hdfs://")) {
      final Configuration configuration = new Configuration();
      if (Utils.logDebugLevel) {
        LOGGER.info(String.format("Attempting to access file located on HDFS: '%s'", fileSpec));
      }
      final FileContext fileContext = FileContext.getFileContext(configuration);
      final Path path = new Path(fileSpec);
      return fileContext.open(path);
    } else {
      if (Utils.logDebugLevel) {
        LOGGER.info(String
            .format("Attempting to access file located on local file system: '%s'", fileSpec));
      }
      return new FileInputStream(fileSpec);
    }
  }

  /*
   * Returns input string with environment variable references expanded, e.g.
   * $SOME_VAR or ${SOME_VAR}
   */
  private static String resolveEnvVars(String input) {
    if (null == input) {
      return null;
    }
    // match ${ENV_VAR_NAME} or $ENV_VAR_NAME
    Pattern p = Pattern.compile("\\$\\{(\\w+)\\}|\\$(\\w+)");
    Matcher m = p.matcher(input); // get a matcher object
    StringBuffer sb = new StringBuffer();
    while (m.find()) {
      String envVarName = null == m.group(1) ? m.group(2) : m.group(1);
      String envVarValue = System.getenv(envVarName);
      m.appendReplacement(sb, null == envVarValue ? envVarName : envVarValue);
    }
    m.appendTail(sb);
    return sb.toString();
  }

  public static void setLogDebugLevel(boolean logDebugLevel) {
    Utils.logDebugLevel = logDebugLevel;
  }

  public static boolean shouldRunForDate(String runDays, Date runDate) {
    return DateUtils.shouldRunForDate(runDays, runDate);
  }

  public static boolean tryParseBoolean(String s, boolean defaultValue) {
    return ParseUtils.tryParseBoolean(s, defaultValue);
  }

  public static int tryParseInt(String s, int defaultValue) {
    return ParseUtils.tryParseInt(s, defaultValue);
  }

  public static long tryParseLong(String s, long defaultValue) {
    return ParseUtils.tryParseLong(s, defaultValue);
  }

  public static void writeOutput(Properties p) throws IOException {
    PropertyUtils.writeToOutput(p);
  }

}
