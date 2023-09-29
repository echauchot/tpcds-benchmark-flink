package org.example.tpcds.flink;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.RowCsvInputFormat;
import org.apache.flink.api.java.io.TextOutputFormat;
import org.apache.flink.api.java.operators.FilterOperator;
import org.apache.flink.api.java.operators.GroupReduceOperator;
import org.apache.flink.api.java.operators.JoinOperator;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.types.Row;

import org.apache.flink.shaded.guava31.com.google.common.base.Strings;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.example.tpcds.flink.CLIUtils.extractParameters;
import static org.example.tpcds.flink.TPCDSUtils.compositeKey;
import static org.example.tpcds.flink.csvSchemas.csvSchemas.RowCsvUtils.FIELD_DELIMITER;
import static org.example.tpcds.flink.csvSchemas.csvSchemas.RowCsvUtils.OrderComparator;
import static org.example.tpcds.flink.csvSchemas.csvSchemas.RowCsvUtils.createInputFormat;


/*
 SELECT dt.d_year, item.i_brand_id brand_id, item.i_brand brand,SUM(ss_ext_sales_price) sum_agg
 FROM  date_dim dt, store_sales, item
 WHERE dt.d_date_sk = store_sales.ss_sold_date_sk
   AND store_sales.ss_item_sk = item.i_item_sk
   AND item.i_manufact_id = 128
   AND dt.d_moy=11
 GROUP BY dt.d_year, item.i_brand, item.i_brand_id
 ORDER BY dt.d_year, sum_agg desc, brand_id
 LIMIT 100
*/

public class Query3ViaFlinkRowDataset {

  public static void main(String[] args) throws Exception {
    final Map<String, String> parameters = extractParameters(args);

    final String pathDateDim = parameters.get("--pathDateDim");
    if (Strings.isNullOrEmpty(pathDateDim)) {
      throw new RuntimeException("Please specify a valid path to 'date_dim' files");
    }
    final String pathStoreSales = parameters.get("--pathStoreSales");
    if (Strings.isNullOrEmpty(pathStoreSales)) {
      throw new RuntimeException("Please specify a valid path to 'store_sales' files");
    }
    final String pathItem = parameters.get("--pathItem");
    if (Strings.isNullOrEmpty(pathItem)) {
      throw new RuntimeException("Please specify a valid path to 'item' files");
    }
    final String pathResults = parameters.get("--pathResults");
    if (Strings.isNullOrEmpty(pathResults)) {
      throw new RuntimeException("Please specify a valid results output directory");
    }

    final String flinkMaster = parameters.get("--flinkMaster");
    if (Strings.isNullOrEmpty(flinkMaster)) {
      throw new RuntimeException("Please specify a valid flinkMaster");
    }

    final String parallelism = parameters.get("--parallelism");
    if (Strings.isNullOrEmpty(parallelism)) {
      throw new RuntimeException("Please specify a valid parallelism");
    }

    final ExecutionEnvironment env;
    if ("[local]".equals(flinkMaster)){
      env = ExecutionEnvironment.createLocalEnvironment();
    } else { // use [auto] that works for cluster execution as well
      env = ExecutionEnvironment.getExecutionEnvironment();
    }
    env.setParallelism(Integer.parseInt(parallelism));

    // Table date_dim
    // SELECT d_date_sk, d_year, d_moy
    int[] selectedFields = new int[]{0, 6, 8};
    final RowCsvInputFormat dateDimInputFormat =
        createInputFormat("date_dim", pathDateDim, selectedFields);
    final FilterOperator<Row> dateDim = env.createInput(dateDimInputFormat)
      // WHERE dt.d_moy=11 AND d_date_sk != null
      .filter(
        (FilterFunction<Row>) value -> value.getField(2) != null && ((Integer) value.getField(2)
          == 11) && (value.getField(0) != null));

    // Table store_sales
    // SELECT ss_sold_date_sk, ss_item_sk, ss_ext_sales_price
    selectedFields = new int[]{0, 2, 15};
    final RowCsvInputFormat storeSalesInputFormat = createInputFormat("store_sales", pathStoreSales, selectedFields);
    final DataSet<Row> storeSales = env
      .createInput(storeSalesInputFormat)
      .filter(
      // WHERE ss_sold_date_sk != null AND ss_item_sk != null
      (FilterFunction<Row>) value -> value.getField(0) != null
        && value.getField(1) != null);

    // Table item
    // SELECT i_item_sk, i_brand_id, i_brand, i_manufact_id
    selectedFields = new int[]{0, 7, 8, 13};
    final RowCsvInputFormat itemInputFormat = createInputFormat("item", pathItem, selectedFields);
    final DataSet<Row> item = env
      .createInput(itemInputFormat)
      // WHERE item.i_manufact_id = 128 AND i_item_sk != null
      .filter((FilterFunction<Row>) value -> value.getField(3) != null
        && (Integer) value.getField(3) == 128 && value.getField(0) != null);

    // Join1: WHERE dt.d_date_sk = store_sales.ss_sold_date_sk
    final JoinOperator<Row, Row, Row> recordsJoinDateSk = dateDim
      .join(storeSales)
      .where(row -> (int)row.getField(0))
      .equalTo(row -> (int)row.getField(0))
      .with((JoinFunction<Row, Row, Row>) Row::join);

    // Join2: WHERE store_sales.ss_item_sk = item.i_item_sk
    final JoinOperator<Row, Row, Row> recordsJoinItemSk = recordsJoinDateSk
      .join(item)
      .where(row -> (int)row.getField(4))
      .equalTo(row -> (int)row.getField(0))
      .with((JoinFunction<Row, Row, Row>) Row::join);

    // GROUP BY dt.d_year, item.i_brand, item.i_brand_id
    final GroupReduceOperator<Row, Row> sum = recordsJoinItemSk
      .groupBy(compositeKey())
      // SUM(ss_ext_sales_price) sum_agg
      .reduceGroup((GroupReduceFunction<Row, Row>) (rows, collector) -> {
        float sumAgg = 0;
        int i = 0;
        Row output = null;

        for (Row row : rows) {
          if (++i == 1) {
            output = row;
          }
          if (row.getField(5) != null) {
            sumAgg += (Float) row.getField(5);
          }
        }
        if (output != null) {
          Row result = new Row(11);
          // need to copy all the fields of one of the input rows (they have the same
          // value of fields on the 3 we need in the final ouput)
          for (int j = 0; j < output.getArity(); j++) {
            result.setField(j, output.getField(j));
          }
          result.setField(10, sumAgg);
          collector.collect(result);
        }
      }).returns(Row.class);

    // ORDER BY dt.d_year, sum_agg desc, brand_id
    final GroupReduceOperator<Row, Row> output = sum
      .reduceGroup((GroupReduceFunction<Row, Row>) (rows, collector) -> {
        List<Row> output1 = new ArrayList<>();
        rows.forEach(output1::add);
        output1.sort(new OrderComparator());
        output1.forEach(collector::collect);
      }).returns(Row.class)
      // LIMIT 100
      .first(100);

    // WRITE d_year|i_brand_id|i_brand|sum_agg
    output.writeAsFormattedText(pathResults, FileSystem.WriteMode.OVERWRITE, new TextOutputFormat.TextFormatter<Row>() {

      @Override public String format(Row row) {
        return row.getField(1) + FIELD_DELIMITER + row.getField(7) + FIELD_DELIMITER + row.getField(
          8) + FIELD_DELIMITER + row.getField(10);
      }
    });
    System.out.print("TPC-DS Query 3 Flink DataSet - start");
    final long start = System.currentTimeMillis();
    env.execute();
    final long end = System.currentTimeMillis();
    final long runTime = (end - start) / 1000;
    System.out.printf(
      "TPC-DS %s - end - %d m %d s. Total: %d%n", "Query 3 ", (runTime / 60), (runTime % 60), runTime);
  }
}
