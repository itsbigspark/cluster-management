package com.bigspark.cloudera.management.common.utils;

public class ParseUtils {

  public static boolean tryParseBoolean(String s, boolean defaultValue) {
    boolean retVal = defaultValue;
    try {
      retVal = Boolean.parseBoolean(s);

    } catch (Exception e) {
    }
    return retVal;
  }

  public static int tryParseInt(String s, int defaultValue) {
    int retVal = defaultValue;
    try {
      retVal = Integer.parseInt(s);
    } catch (Exception e) {
    }
    return retVal;
  }

  public static long tryParseLong(String s, long defaultValue) {
    long retVal = defaultValue;
    try {
      retVal = Long.parseLong(s);
    } catch (Exception e) {
    }
    return retVal;
  }
}
