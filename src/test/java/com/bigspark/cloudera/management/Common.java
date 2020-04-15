package com.bigspark.cloudera.management;

import com.bigspark.cloudera.management.common.utils.DateUtils;
import com.bigspark.cloudera.management.common.utils.PropertyUtils;
import java.io.IOException;
import java.text.ParseException;
import java.util.Calendar;
import java.util.Properties;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class Common {

  private static final Log LOGGER = LogFactory.getLog(Common.class);

//	public static void testHdfsConnection(Configuration conf) throws IOException {
//		FileSystem fs = FileSystem.get(conf);
//		GetFiles getFiles = new GetFiles(fs.getHomeDirectory().toString());
//
//		LOGGER.info(String.format("Getting files for %s", (fs.getHomeDirectory().getName())));
//		FileStatus[] files = getFiles.execute(fs);
//		for (FileStatus currFile : files) {
//			LOGGER.info(currFile);
//		}
//	}

  private static Properties testProperties;

  public synchronized static Properties getIntegrationTestProperties() throws IOException {
    if (testProperties == null) {
      testProperties = PropertyUtils
          .getPropertiesFile("src/main/resources/integration-test.properties");
    }
    return testProperties;
  }

//	private static SecurityHelperInstance testSecurityHelper;
//	public synchronized static SecurityHelperInstance getSecurityHelper() throws IOException {
//		if(testSecurityHelper == null) {
//			Properties props = Common.getIntegrationTestProperties();
//			String keytabFileLocation = props.getProperty("keyTabFileLocation");
//			String principalName = props.getProperty("principalName");
//			testSecurityHelper = new SecurityHelperInstance(principalName, keytabFileLocation);
//		}
//		return testSecurityHelper;
//	}

//	public static void logUserGroupInformation(UserGroupInformation ugi) throws IOException {
//		LOGGER.info(String.format("isFromKeytab: %s", ugi.isFromKeytab()));
//		LOGGER.info(String.format("isLoginKeytabBased: %s", UserGroupInformation.isLoginKeytabBased()));
//		LOGGER.info(String.format("isLoginTicketBased: %s", UserGroupInformation.isLoginTicketBased()));
//		LOGGER.info(String.format("userGroupInformation: %s", ugi));
//	}

//	public static void setLoginUser(UserGroupInformation ugi) throws IOException {
//		SecurityHelper.loginUserFromUgi(ugi);
//		LOGGER.info(String.format("isLoginKeytabBased: %s", UserGroupInformation.isLoginKeytabBased()));
//		LOGGER.info(String.format("isLoginTicketBased: %s", UserGroupInformation.isLoginTicketBased()));
//	}

  public static String getBannerStart(String testName) throws ParseException {
    StringBuilder sb = new StringBuilder();
    sb.append(
        "/******************************************************************************/\r\n");
    sb.append(String.format("	Test: %s\r\n", testName));
    sb.append(String.format("	Start Time:%s\r\n",
        DateUtils.getFormattedDateTime(Calendar.getInstance().getTime())));
    sb.append(
        "/******************************************************************************/\r\n");
    return sb.toString();
  }

  public static String getBannerFinish(String testName) throws ParseException {
    StringBuilder sb = new StringBuilder();

    sb.append(
        "/******************************************************************************/\r\n");
    sb.append(String.format("	Test: %s\r\n", testName));
    sb.append(String.format("	End Time:%s\r\n",
        DateUtils.getFormattedDateTime(Calendar.getInstance().getTime())));
    sb.append(
        "/******************************************************************************/\r\n");
    sb.append("\r\n\r\n");
    return sb.toString();
  }


}
