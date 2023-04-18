package org.example.tpcds.flink;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.types.Row;

class TPCDSUtils {
  static KeySelector<Row, String> compositeKey() {
    return row -> String.valueOf(row.getField(1)) + String.valueOf(row.getField(8)) + String.valueOf(row.getField(7));
  }


}
