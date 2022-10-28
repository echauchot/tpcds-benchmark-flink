package org.example.tpcds.flink;

import java.util.HashMap;
import java.util.Map;

public class CLIUtils {
  public static Map<String, String> extractParameters(String[] args) {
    Map<String, String> result = new HashMap<>();
    for (String arg : args) {
      final String key = arg.split("=")[0];
      final String value = arg.split("=")[1];
      result.put(key, value);
    }
    return result;
  }
}
