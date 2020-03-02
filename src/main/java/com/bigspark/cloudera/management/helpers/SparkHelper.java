package com.bigspark.cloudera.management.helpers;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.spark.sql.SparkSession;

/**
 * Entry point facade class to aid with complex operations within a SparkSession not supported by Spark DataFrame operations.
 * Examples include
 *   1. HDFS operations (read, write delete files, manage directories etc.)
 *   2. Security operations (read and write to secure JCEKS stores)
 * @author Rich Hay
 *
 */
public class SparkHelper {
	
	/**
	 * Nested Facade class to expose HDFS operations to a Spark Session
	 * @author Rich Hay
	 *
	 */
	public static class Hdfs {
		/**
		 * Method to append content to a file
		 * @param targetPath The path of the file to which content will be appended
		 * @param fileName The name of the file to which content will be appended
		 * @param payload The content to append
		 * @throws IllegalArgumentException
		 * @throws IOException
		 */
		public static void appendFileContent(String targetPath, String fileName, String payload) throws IllegalArgumentException, IOException {
			FileSystemHelper.writeFileContent(targetPath, fileName, payload, false);
		}
		
		/**
		 * Method to read all content from an HDFS file
		 * @param path The path of the file to read content from
		 * @return A String containing the file content
		 * @throws IllegalArgumentException
		 * @throws IOException
		 * @throws InterruptedException
		 */
		public static String getFileContent(String path) throws IllegalArgumentException, IOException, InterruptedException {
			return FileSystemHelper.getFileContent(path);
		}
		
		/**
		 * Method to (over)write conent to a file - if the file exists it will be overwritten
		 * @param targetPath The path of the file to which content will be appended
		 * @param fileName The name of the file to which content will be appended
		 * @param payload The content to append
		 * @throws IllegalArgumentException
		 * @throws IOException
		 */
		public static void 	writeFileContent(String targetPath, String fileName, String payload) throws IllegalArgumentException, IOException {
			FileSystemHelper.writeFileContent(targetPath, fileName, payload, true);
		}
		
		/**
		 * Method to get a users homearea
		 * @return
		 * @throws IOException
		 */
		public static String getUserHomeArea() throws IOException {
			return FileSystemHelper.getUserHomeArea();
		}
		
		/**
		 * Method to copy files from the local file system to HDFS
		 * @param srcPath The Source local FS file or directory
		 * @param tgtPath The Target HDFS file or directory
		 * @throws IOException
		 */
		public static void copyFromLocal(String srcPath, String tgtPath) throws IOException {
			FileSystemHelper.copyFromLocal(srcPath, tgtPath);
		}
		
		/**
		 * Method to copy files from HDFS to the local file system
		 * @param srcPath The Source HDFS file or directory
		 * @param tgtPath The Target local FS file or directory
		 * @throws IOException
		 */
		public static void CopyToLocal(String srcPath, String tgtPath) throws IOException {
			FileSystemHelper.copyToLocal(srcPath, tgtPath);
		}
		
		/**
		 * Method to delete a file or folder - non recursive
		 * @param path The Path to delete
		 * @throws IOException
		 */
		public static void delete(String path) throws IOException {
			FileSystemHelper.delete(path, false);
		}
		
		/**
		 * Method to delete a file or folder
		 * @param path The Path to delete
		 * @param recursive Whether to delete recursively or not
		 * @throws IOException
		 */
		public static void delete(String path, Boolean recursive) throws IOException {
			FileSystemHelper.delete(path, recursive);
		}
		
		/**
		 * Method to move (rename) a path
		 * @param srcPath The Path to move from
		 * @param tgtPath The Path to move to
		 * @throws IOException
		 */
		public static void move(String srcPath, String tgtPath) throws IOException {
			FileSystemHelper.move(srcPath, tgtPath);
		}
		
		/**
		 * Method to list the contents of a path
		 * @param path The path on which to perform the list operation 
		 * @return A list of Path Strings
		 * @throws IllegalArgumentException
		 * @throws IOException
		 */
		public static ArrayList<String> list(String path) throws IllegalArgumentException, IOException {
			return FileSystemHelper.list(path);
		}
	}
	
	/**
	 * Nested facade class to expose Security related operations to a SparkSession
	 * @author Rich Hay
	 *
	 */
	public static class Security {
		/**
		 * Method to list aliases within the specified keystore
		 * @param pathToKeyStore The path to the keystore
		 * @return A List of aliases
		 * @throws URISyntaxException
		 * @throws IOException
		 * @throws InterruptedException
		 */
		public static List<String> getAliasesFromHadoopKeyStore(String pathToKeyStore) throws URISyntaxException, IOException, InterruptedException {
			return HadoopKeyStoreHelper.getAliasesFromHadoopKeyStore(SparkHelper.getSparkSession(), pathToKeyStore);
		}
		
		/**
		 * Method to expose UserGroupInformation.getCurrentUser()
		 * @return 
		 * @throws IOException
		 */
		public static String getCurrentUser() throws IOException {
			return UserGroupInformation.getCurrentUser().getUserName();
		}
		
		/**
		 * Method to get the encrypted credential for the supplied alias from the specified keystore
		 * @param pathToKeyStore The location of the keystore
		 * @param passwordAlias The alias for the credential of the keystore
		 * @return The unencrypted value
		 * @throws URISyntaxException
		 * @throws IOException
		 * @throws InterruptedException
		 */
		public static String getFromHadoopKeyStore(String pathToKeyStore, String passwordAlias) throws URISyntaxException, IOException, InterruptedException {
			return HadoopKeyStoreHelper.getFromHadoopKeyStore(SparkHelper.getSparkSession(), pathToKeyStore, passwordAlias);
		}
		
		/**
		 * Method to expose UserGroupInformation.getLoginUser()
		 * @return
		 * @throws IOException
		 */
		public static String getLoginUser() throws IOException {
			return UserGroupInformation.getLoginUser().getUserName();
		}
		
		/**
		 * Method to encrpy and write a credentia into a keystore using the supplied alias
		 * @param pathToKeyStore The path to the keystore
		 * @param passwordAlias The alias of the credential
		 * @param password The credential value
		 * @throws URISyntaxException
		 * @throws IOException
		 * @throws InterruptedException
		 */
		public static void writeToHadoopKeyStore(String pathToKeyStore, String passwordAlias, String password) throws URISyntaxException, IOException, InterruptedException {
			HadoopKeyStoreHelper.writeToHadoopKeyStore(SparkHelper.getSparkSession(), pathToKeyStore, passwordAlias, password);
		}
	}
	
	/**
	 * Lazily loaded SparkSession
	 */
	private static SparkSession sparkSession = null;
	
	/**
	 * Mehtod to return an Impala Helper for Non-Query related JDBC actions
	 * @param connectionString The connection String for Impala
	 * @return The Impala Helper
	 */
	public static ImpalaHelper getImpalaHelper(String connectionString) {
		return new ImpalaHelper(connectionString);
	}
	
	/**
	 * Method to return the Spark Application ID used in logs etc.
	 * @return Application ID
	 */
	public static String getSparkApplicationId() {
		return getSparkSession().sparkContext().applicationId();
	}
	
	/**
	 * Method to Find the SparkSession within which we are operation
	 * @return SparkSession created by spark-submit within which this process is running
	 */
	public synchronized static SparkSession getSparkSession() {
		if(sparkSession == null) {
			sparkSession = SparkSession.builder()
					.enableHiveSupport()
					.getOrCreate();
		}
		return sparkSession;
	}
	
	/**
	 * Method to get the Spark username (which is the "Executing user")
	 * @return Spark User name
	 */
	public static String getSparkUser() {
		return SparkHelper.getSparkSession().sparkContext().sparkUser();
	}
	
	/**
	 * Method to allow multiline Strings to be displayed in Zeppelin output
	 * @param payload The String to work on
	 * @return
	 */
	public static String toZeppelinOutput(String payload) {
		return String.format("%%html %s", payload.replace("\r\n", "<br/>").replace("\r", "<br/>").replace("\n", "<br/>"));
	}
}
