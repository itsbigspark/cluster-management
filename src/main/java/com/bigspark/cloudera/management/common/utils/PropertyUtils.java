package com.bigspark.cloudera.management.common.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PropertyUtils {
	public static String dumpProperties(final Properties properties) {
		int maximumKeyLength = 1;
		for (final Object key : properties.keySet()) {
			final int keyLength = ((String) key).length();
			if (keyLength > maximumKeyLength) {
				maximumKeyLength = keyLength;
			}
		}
		final String keyFormat = "%" + maximumKeyLength + "s";
		final StringBuilder stringBuilder = new StringBuilder();
		for (final Object key : properties.keySet()) {
			stringBuilder.append("\n").append(String.format(keyFormat, key)).append(" = ")
					.append(properties.getProperty((String) key));
		}
		return stringBuilder.toString();
	}

	public static String getOptionalProperty(Properties properties, String key, String defaultValue) {
		if (properties.containsKey(key)) {
			return properties.getProperty(key);
		} else {
			return defaultValue;
		}
	}

	@SuppressWarnings("unchecked")
	public static <T> T getOptionalProperty(Properties properties, String key, T defaultValue) {
		String strVal = defaultValue.toString();
		try {
			if (properties.containsKey(key)) {
				strVal = properties.getProperty(key);
				Class<?> classType = defaultValue.getClass();
				if (classType.isAssignableFrom(String.class)) {
					return (T) strVal;
				} else if (classType.isAssignableFrom(Integer.class)) {
					return (T) Integer.valueOf(strVal);
				} else if (classType.isAssignableFrom(Boolean.class)) {
					return (T) Boolean.valueOf(strVal);
				}
			}
		} catch (Exception e) {
			// do nothing
		}

		return defaultValue;
	}

	public static Properties getProgramArgsAsProps(String[] args) {
		Properties props = new Properties();
		for (String currArg : args) {
			if (currArg.contains("=")) {
				int position = currArg.indexOf('=');
				String key = currArg.split("=")[0];
				// String key = currArg.substring(1,position);
				// String value = currArg.split("=")[1];
				String value = currArg.substring(position + 1);
				props.setProperty(key, value);
			}
		}
		return props;
	}

	public static Properties getPropertiesFile(InputStream props) throws IOException {
		// Load Properties File
		final Properties properties = new Properties();
		properties.load(props);
		return properties;
	}

	public static Properties getPropertiesFile(String propertiesFileSpec) throws IOException {
		// Load Properties File
		final Properties properties = new Properties();
		final Properties parsedProperties = new Properties();
		properties.load(openFile(propertiesFileSpec));
		for (String key : properties.stringPropertyNames()) {
			String value = resolveEnvVars(properties.getProperty(key));
			parsedProperties.put(key, value);
		}
		return parsedProperties;
	}

	public static Properties getPropertiesFromConfigDirectoryVariable(String configDirectorySystemVariable)
			throws IOException {
		String configLocation = System.getenv(configDirectorySystemVariable);
		Properties loadedProperties = new Properties();
		if (configLocation != null && !configLocation.trim().equals("")) {
			Path path = Paths.get(configLocation);
			if (Files.exists(path) && Files.isDirectory(path)) {
				loadedProperties = getPropertyFiles(configLocation);
			}
		}
		return loadedProperties;
	}

	public static Properties getPropertyFiles(String path) throws IOException {
		Properties allProperties = new Properties();
		List<Path> configFiles = FileSystemUtils.listFiles(Paths.get(path));
		for (Path currPath : configFiles) {
			if (currPath.getFileName().toString().endsWith(".properties")) {
				Properties currProperties = getPropertiesFile(currPath.toString());
				allProperties.putAll(currProperties);
			}
		}
		return allProperties;
	}

	public static boolean getPropertyValue(Properties props, String key, boolean defaultValue) {
		if (props.containsKey(key)) {
			return ParseUtils.tryParseBoolean(props.getProperty(key), defaultValue);
		} else {
			return defaultValue;
		}
	}

	public static String getPropertyValue(Properties props, String key, String defaultValue) {
		if (props.containsKey(key)) {
			return props.getProperty(key);
		} else {
			return defaultValue;
		}
	}

	public static Properties mergePropertyFiles(Properties primaryProperties, Properties secondaryProperties) {
		return mergePropertyFiles(primaryProperties, secondaryProperties, null);
	}

	public static Properties mergePropertyFiles(Properties primaryProperties, Properties secondaryProperties,
			HashMap<String, String> ignorePatterns) {
		// Enumerate secondary keys to apply to primary set
		for (String key : secondaryProperties.stringPropertyNames()) {
			String value = secondaryProperties.getProperty(key);
			// Ensure we do not want to ignore this key (e.g. workspace_ stuff
			// in oozie properties)
			if (ignorePatterns != null && !(ignorePatterns.size() == 0)) {
				for (String ignorePatternKey : ignorePatterns.keySet()) {
					if (key.matches(ignorePatternKey)) {
						continue;
					}
				}
			}
			// Add the key/value to the Primary set if it does not
			if (!primaryProperties.containsKey(key)) {
				primaryProperties.put(key, value);
			}
		}
		return primaryProperties;

	}

	public static InputStream openFile(final String fileSpec) throws IOException {
		if (fileSpec.startsWith("hdfs://")) {
			// final org.apache.hadoop.conf.Configuration configuration = new
			// org.apache.hadoop.conf.Configuration();
			// final FileContext fileContext =
			// FileContext.getFileContext(configuration);
			// final org.apache.hadoop.fs.Path path = new
			// org.apache.hadoop.fs.Path(fileSpec);
			// return fileContext.open(path);
			return null;
		} else {

			return new FileInputStream(fileSpec);
		}
	}

	public static String resolveEnvVars(String input) {
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

	public static void writeToOutput(Properties p) throws IOException {
		// return PropertyUtils.
		// File file
		// = new File(System.getProperty("oozie.action.output.properties"));
		String oozieProp = System.getProperty("oozie.action.output.properties");
		OutputStream os = null;
		if (oozieProp != null) {
			// System.out.print(String.format("oozie.action.output.properties=%s",oozieProp));
			File propFile = new File(oozieProp);
			os = new FileOutputStream(propFile);
			p.store(os, "");
			os.close();
		} else {
			System.out.print(p.toString());
		}
	}
}
