package org.example.tpcds.flink;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.io.RowCsvInputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.shaded.guava30.com.google.common.base.Strings;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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

public class Query3ViaFlinkRowDatastream {

  private static final Logger LOG = LogManager.getLogger(Query3ViaFlinkRowDataset.class);
  private static final String FIELD_DELIMITER = "|";
  private static final String ROW_DELIMITER = "\n";

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

    final StreamExecutionEnvironment env;
    if ("[local]".equals(flinkMaster)){
      env = StreamExecutionEnvironment.createLocalEnvironment();
    } else { // use [auto] that works for cluster execution as well
      env = StreamExecutionEnvironment.getExecutionEnvironment();
    }
    env.setParallelism(Integer.parseInt(parallelism));
    env.setRuntimeMode(RuntimeExecutionMode.BATCH);

    // Table date_dim
    // SELECT d_date_sk, d_year, d_moy
    int[] selectedFields = new int[]{0, 6, 8};
    final RowCsvInputFormat dateDimInputFormat =
      createInputFormat("date_dim", pathDateDim, selectedFields);
    final SingleOutputStreamOperator<Row> dateDim = env.createInput(dateDimInputFormat)
      // WHERE dt.d_moy=11 AND d_date_sk != null
      .filter(
        (FilterFunction<Row>) value -> value.getField(2) != null && ((Integer) value.getField(2)
          == 11) && (value.getField(0) != null));

    // Table store_sales
    // SELECT ss_sold_date_sk, ss_item_sk, ss_ext_sales_price
    selectedFields = new int[]{0, 2, 15};
    final RowCsvInputFormat storeSalesInputFormat = createInputFormat("store_sales", pathStoreSales, selectedFields);
    final SingleOutputStreamOperator<Row> storeSales = env.
      createInput(storeSalesInputFormat)
      .filter(
      // WHERE ss_sold_date_sk != null AND ss_item_sk != null
      (FilterFunction<Row>) value -> value.getField(0) != null && value.getField(1) != null);

    // Table item
    // SELECT i_item_sk, i_brand_id, i_brand, i_manufact_id
    selectedFields = new int[]{0, 7, 8, 13};
    final RowCsvInputFormat itemInputFormat = createInputFormat("item", pathItem, selectedFields);
    final SingleOutputStreamOperator<Row> item = env
      .createInput(itemInputFormat)
      // WHERE item.i_manufact_id = 128 AND i_item_sk != null
      .filter((FilterFunction<Row>) value -> value.getField(3) != null
        && (Integer) value.getField(3) == 128 && value.getField(0) != null);

    // Join1: WHERE dt.d_date_sk = store_sales.ss_sold_date_sk
    final DataStream<Row> recordsJoinDateSk = dateDim
      .join(storeSales)
      .where(row -> (int) row.getField(0))
      .equalTo(row -> (int) row.getField(0))
      .window(GlobalWindows.create())
      .apply((JoinFunction<Row, Row, Row>) Row::join);

    // Join2: WHERE store_sales.ss_item_sk = item.i_item_sk
    final DataStream<Row> recordsJoinItemSk = recordsJoinDateSk
      .join(item)
      .where(row -> (int) row.getField(4))
      .equalTo(row -> (int) row.getField(0))
      .window(GlobalWindows.create())
      .apply((JoinFunction<Row, Row, Row>) Row::join);


    // GROUP BY dt.d_year, item.i_brand, item.i_brand_id
    final SingleOutputStreamOperator<Row> sum = recordsJoinItemSk
      .keyBy(compositeKey())
      // SUM(ss_ext_sales_price) sum_agg
      .reduce((ReduceFunction<Row>) (row1, row2) -> {
        {
          // d_year, brand_id, brand, sum_agg
          Row output = new Row(4);
          output.setField(0, row1.getField(1));
          output.setField(1, row1.getField(7));
          output.setField(2, row1.getField(8));
          final Float record1Price = (Float) row1.getField(5);
          final Float record2Price = (Float) row2.getField(5);
          if (record1Price != null && record2Price != null) {
            output.setField(3, record1Price + record2Price);
          } else if (record1Price == null) {
            output.setField(3, record2Price);
          } else {
            output.setField(3, record1Price);
          }
          return output;
        }
      }).returns(Row.class);
    // ORDER BY dt.d_year, sum_agg desc, brand_id
    final SingleOutputStreamOperator<Row> output = sum
      .windowAll(GlobalWindows.create())
      .process(new ProcessAllWindowFunction<Row, Row, GlobalWindow>() {

        @Override public void process(
          ProcessAllWindowFunction<Row, Row, GlobalWindow>.Context context, Iterable<Row> rows,
          Collector<Row> collector) {
          List<Row> output = new ArrayList<>();
          rows.forEach(output::add);
          output.sort(new OrderComparator());
          output.forEach(collector::collect);
        }
      }).returns(Row.class)
      // LIMIT 100
      .keyBy(
        (KeySelector<Row, Integer>) record -> 0)// key is required for stateful count (limit 100 impl), use fake 0 key
      .map(new LimitMapper());
    // WRITE d_year|i_brand_id|i_brand|sum_agg
    // parallelism is 1 so it does not mess the order up
    output.addSink(StreamingFileSink.forRowFormat(new Path(pathResults), new Encoder<Row>() {

      @Override public void encode(Row row, OutputStream outputStream)
        throws IOException {
        String output = row.getField(0) + FIELD_DELIMITER + row.getField(1) + FIELD_DELIMITER + row.getField(
          2) + FIELD_DELIMITER + row.getField(3);
        outputStream.write(output.getBytes(StandardCharsets.UTF_8));
        outputStream.write(10);
      }
    }).build());
    LOG.info("TPC-DS Query 3 Flink DataStream - start");
    final long start = System.currentTimeMillis();
    env.execute();
    final long end = System.currentTimeMillis();
    final long runTime = (end - start) / 1000;
    LOG.info(
      "TPC-DS {} - end - {}m {}s. Total: {}", "Query 3", (runTime / 60), (runTime % 60), runTime);
    System.out.println(String.format("TPC-DS %s - end - %d m %d s. Total: %d", "Query 3 ", (runTime / 60), (runTime % 60), runTime));
  }

  private static KeySelector<Row, String> compositeKey() {
    return row -> String.valueOf(row.getField(1)) + row.getField(8) + row.getField(7);
  }

  private static Map<String, String> extractParameters(String[] args) {
    Map<String, String> result = new HashMap<>();
    for (String arg : args) {
      final String key = arg.split("=")[0];
      final String value = arg.split("=")[1];
      result.put(key, value);
    }
    return result;
  }

  private static RowCsvInputFormat createInputFormat(String tableName, String filePath, int[] selectedFields) {
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

  private static class OrderComparator implements Comparator<Row> {

    @Override
    public int compare(Row a, Row b) {
      //ORDER BY dt.d_year, sum_agg desc, brand_id
      int aDYear = (int) a.getField(0);
      int bDYear = (int) b.getField(0);
      if (bDYear != aDYear) {
        return aDYear - bDYear;
      }

      float aSumAgg = (Float) a.getField(3);
      float bSumAgg = (Float) b.getField(3);
      if (bSumAgg != aSumAgg) {
        return bSumAgg > aSumAgg ? 1 : -1;
      }

      int aIBrandId = (int) a.getField(1);
      int bIBrandId = (int) b.getField(1);
      return aIBrandId - bIBrandId;
    }
  }

  private static class LimitMapper extends RichMapFunction<Row, Row> {

    ValueState<Integer> state;

    @Override
    public void open(Configuration parameters) {
      state = getRuntimeContext().getState(new ValueStateDescriptor<>("countState", Integer.class));
    }

    @Override
    public Row map(Row row) throws Exception {
      Integer count = state.value();
      if (count == null){
        count = 0;
      }
      state.update(++count);
      if (count > 100){
        close();
      }
      return row;
    }
  }

}

