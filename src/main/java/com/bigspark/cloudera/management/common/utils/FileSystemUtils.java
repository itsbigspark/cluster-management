package com.bigspark.cloudera.management.common.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import org.apache.hadoop.fs.FileContext;

public class FileSystemUtils {

  /**
   * Method to locate a file on the classpath Stolen wholesale from https://examples.javacodegeeks.com/core-java/class/find-a-file-in-classpath/
   *
   * @param fileName - The name of the file to locate on the class path
   * @return
   */
  public static File findFileOnClassPath(final String fileName) {
    final String classpath = System.getProperty("java.class.path");
    final String pathSeparator = System.getProperty("path.separator");
    final StringTokenizer tokenizer = new StringTokenizer(classpath, pathSeparator);

    while (tokenizer.hasMoreTokens()) {
      final String pathElement = tokenizer.nextToken();
      final File directoryOrJar = new File(pathElement);
      final File absoluteDirectoryOrJar = directoryOrJar.getAbsoluteFile();

      if (absoluteDirectoryOrJar.isFile()) {
        final File target = new File(absoluteDirectoryOrJar.getParent(), fileName);
        if (target.exists()) {
          return target;
        }
      } else {
        final File target = new File(directoryOrJar, fileName);
        if (target.exists()) {
          return target;
        }
      }
    }
    return null;
  }

  public static String getFile(final String fileSpec) throws IOException {
    return org.apache.commons.io.IOUtils.toString(openFile(fileSpec));
  }

  public static List<Path> listFiles(Path path) throws IOException {
    return listFiles(path, false);
  }

  public static List<Path> listFiles(Path path, Boolean recursive) throws IOException {
    List<Path> files = new ArrayList<>();
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(path)) {
      for (Path entry : stream) {
        if (Files.isDirectory(entry)) {
          List<Path> childFiles = listFiles(entry, recursive);
          files.addAll(childFiles);
        }
        files.add(entry);
      }
    }
    return files;
  }

  public static InputStream openFile(final String fileSpec) throws IOException {
    if (fileSpec.startsWith("hdfs://")) {
      final org.apache.hadoop.conf.Configuration configuration = new org.apache.hadoop.conf.Configuration();
      final FileContext fileContext = FileContext.getFileContext(configuration);
      final org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(fileSpec);
      return fileContext.open(path);
    } else {
      return new FileInputStream(fileSpec);
    }
  }

}
