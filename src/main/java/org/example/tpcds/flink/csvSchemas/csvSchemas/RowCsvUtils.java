package org.example.tpcds.flink.csvSchemas.csvSchemas;

import java.util.Comparator;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.io.RowCsvInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.types.Row;

public class RowCsvUtils {
  public static final String FIELD_DELIMITER = "|";
  public static final String ROW_DELIMITER = "\n";

  public static RowCsvInputFormat createInputFormat(String tableName, String filePath, int[] selectedFields) {
    TypeInformation[] fieldTypes;
    switch (tableName) {
      case "date_dim":
        fieldTypes = new TypeInformation[] {
          BasicTypeInfo.INT_TYPE_INFO,
          BasicTypeInfo.INT_TYPE_INFO,
          BasicTypeInfo.INT_TYPE_INFO
        };
        break;
      case "store_sales":
        fieldTypes = new TypeInformation[] {
          BasicTypeInfo.INT_TYPE_INFO,
          BasicTypeInfo.INT_TYPE_INFO,
          BasicTypeInfo.FLOAT_TYPE_INFO
        };
        break;
      case "item":
        fieldTypes = new TypeInformation[] {
          BasicTypeInfo.INT_TYPE_INFO,
          BasicTypeInfo.INT_TYPE_INFO,
          BasicTypeInfo.STRING_TYPE_INFO,
          BasicTypeInfo.INT_TYPE_INFO
        };
        break;
      default:
        throw new IllegalStateException(tableName + " unsupported");
    }
    return new RowCsvInputFormat(
      new Path(filePath), fieldTypes, ROW_DELIMITER, FIELD_DELIMITER, selectedFields);
  }
  public static class OrderComparator implements Comparator<Row> {

    @Override
    public int compare(Row a, Row b) {
      //ORDER BY dt.d_year, sum_agg desc, brand_id
      int aDYear = (int) a.getField(1);
      int bDYear = (int) b.getField(1);
      if (bDYear != aDYear) {
        return aDYear - bDYear;
      }

      float aSumAgg = (Float) a.getField(10);
      float bSumAgg = (Float) b.getField(10);
      if (bSumAgg != aSumAgg) {
        return bSumAgg > aSumAgg ? 1 : -1;
      }

      int aIBrandId = (int) a.getField(7);
      int bIBrandId = (int) b.getField(7);
      return aIBrandId - bIBrandId;
    }
  }

}
