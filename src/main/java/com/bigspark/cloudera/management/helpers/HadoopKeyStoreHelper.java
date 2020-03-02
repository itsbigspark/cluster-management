package com.bigspark.cloudera.management.helpers;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.alias.CredentialProvider;
import org.apache.hadoop.security.alias.CredentialProvider.CredentialEntry;
import org.apache.hadoop.security.alias.CredentialProviderFactory;
import org.apache.spark.sql.SparkSession;


public class HadoopKeyStoreHelper {
	
	public static List<String> getAliasesFromHadoopKeyStore(SparkSession spark, String pathToKeyStore) throws URISyntaxException, IOException, InterruptedException {
		CredentialProvider cp = HadoopKeyStoreHelper.getCredentialProvider(spark, pathToKeyStore);
		return cp.getAliases();
	}
	

	public static String getFromHadoopKeyStore(SparkSession spark, String pathToKeyStore, String passwordAlias)
			throws URISyntaxException, IOException, InterruptedException {
		CredentialProvider cp = HadoopKeyStoreHelper.getCredentialProvider(spark, pathToKeyStore);
		CredentialEntry ce = cp.getCredentialEntry(passwordAlias);
		return String.valueOf(ce.getCredential());
	}

	
	// https://github.com/c9n/hadoop/blob/master/hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/security/alias/JavaKeyStoreProvider.java
	// jceks://hdfs@nn1.example.com/my/creds.jceks ->
	// hdfs://nn1.example.com/my/creds.jceks
	// jceks://file/home/rich/creds.jceks -> file:///home/rich/creds.jceks
	public static void writeToHadoopKeyStore(SparkSession spark, String pathToKeyStore, String passwordAlias, String password)
			throws URISyntaxException, IOException, InterruptedException {
		CredentialProvider cp = HadoopKeyStoreHelper.getCredentialProvider(spark, pathToKeyStore);
		if (cp.getAliases().contains(passwordAlias)) {
			cp.deleteCredentialEntry(passwordAlias);
		}
		cp.createCredentialEntry(passwordAlias, password.toCharArray());
		cp.flush();
	}

	private static URI getPathInUriFormat(String path) throws URISyntaxException {
		if(!path.startsWith("jceks://hdfs" )) {
			return new URI(String.format("jceks://hdfs@%s", path));
		} else {
			return new URI(path);
		}
	}
	
	private static CredentialProvider getCredentialProvider(SparkSession spark, String pathToKeyStore) throws URISyntaxException, IOException {
		URI location = getPathInUriFormat(pathToKeyStore);
		Configuration conf = spark.sparkContext().hadoopConfiguration();
		conf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH, location.toString());
		CredentialProvider cp = CredentialProviderFactory.getProviders(conf).get(0);
		return cp;
	}
}
