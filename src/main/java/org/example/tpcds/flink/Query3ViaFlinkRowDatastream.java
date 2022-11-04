package org.example.tpcds.flink;

import static org.example.tpcds.flink.CLIUtils.extractParameters;
import static org.example.tpcds.flink.csvSchemas.csvSchemas.RowCsvUtils.FIELD_DELIMITER;
import static org.example.tpcds.flink.csvSchemas.csvSchemas.RowCsvUtils.createInputFormat;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Map;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.io.RowCsvInputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.shaded.guava30.com.google.common.base.Strings;
import org.apache.flink.shaded.guava30.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.example.tpcds.flink.csvSchemas.csvSchemas.RowCsvUtils;

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
    final DataStream<Row> storeSales = env
      .createInput(storeSalesInputFormat)
      .filter(
      // WHERE ss_sold_date_sk != null AND ss_item_sk != null
      (FilterFunction<Row>) value -> value.getField(0) != null && value.getField(1) != null);

    // Table item
    // SELECT i_item_sk, i_brand_id, i_brand, i_manufact_id
    selectedFields = new int[]{0, 7, 8, 13};
    final RowCsvInputFormat itemInputFormat = createInputFormat("item", pathItem, selectedFields);
    final DataStream<Row> item = env
      .createInput(itemInputFormat)
      // WHERE item.i_manufact_id = 128 AND i_item_sk != null
      .filter((FilterFunction<Row>) value -> value.getField(3) != null
        && (Integer) value.getField(3) == 128 && value.getField(0) != null);

    // Join1: WHERE date_dim.d_date_sk = store_sales.ss_sold_date_sk
    final DataStream<Row> recordsJoinDateSk = dateDim
      .keyBy((KeySelector<Row, Integer>) value -> (Integer) value.getField(0))
      .connect(storeSales.keyBy(
        (KeySelector<Row, Integer>) value -> (Integer) value.getField(0)))
      .process(new JoinRows())
      .returns(Row.class);

    // Join2: WHERE store_sales.ss_item_sk = item.i_item_sk
    final DataStream<Row> recordsJoinItemSk = recordsJoinDateSk
      .keyBy((KeySelector<Row, Integer>) value -> (Integer) value.getField(4))
      .connect(
        item.keyBy((KeySelector<Row, Integer>) value -> (Integer) value.getField(0)))
      .process(new JoinRows())
      .returns(Row.class);

    // GROUP BY date_dim.d_year, item.i_brand, item.i_brand_id
    final DataStream<Row> sum =
        recordsJoinItemSk
            .keyBy(compositeKey())
            // SUM(ss_ext_sales_price) sum_agg
            .reduce(
                (ReduceFunction<Row>)
                    (row1, row2) -> {
                      {
                        Row output = new Row(11);
                        // rows are incrementally merged so we can receive one that was already reduced
                        // (arrity 11) in that case we need to take the aggregated sum.
                        Float sum1 = row1.getArity() == 11 ? (Float)row1.getField(10) : (Float)row1.getField(5);
                        Float sum2 = row2.getArity() == 11 ? (Float)row2.getField(10) : (Float)row2.getField(5);
                        // copy all the fields except the sumAgg
                        for (int i = 0; i < 10; i++) {
                          output.setField(i, row1.getField(i));
                        }

                        output.setField(10, (sum1 != null ? sum1 : 0.0f) + (sum2 != null ? sum2 : 0.0f));
                        return output;
                      }
                    })
            .returns(Row.class);
    // ORDER BY dt.d_year, sum_agg desc, brand_id

    final DataStream<Row> output = sum
      .keyBy((KeySelector<Row, Integer>) row -> 0) //key is required for stateful sort
      .process(new KeyedProcessFunction<Integer,Row, Row>() {

        ListState<Row> rows;
        @Override
        public void processElement(Row row,
          KeyedProcessFunction<Integer, Row, Row>.Context context, Collector<Row> collector)
          throws Exception {
          rows.add(row);
          context.timerService().registerProcessingTimeTimer(Long.MAX_VALUE);
        }

        @Override
        public void open(Configuration parameters) {
          rows = getRuntimeContext().getListState(new ListStateDescriptor<>("rows", Row.class));
        }

        @Override public void onTimer(long timestamp,
          KeyedProcessFunction<Integer, Row, Row>.OnTimerContext context, Collector<Row> collector)
          throws Exception {
          final Iterable<Row> storedRows = rows.get();
          ArrayList<Row> sortedRows = Lists.newArrayList(storedRows);
          sortedRows.sort(new RowCsvUtils.OrderComparator());
          sortedRows.forEach(collector::collect);
        }

      })

      // LIMIT 100
      .keyBy(
        (KeySelector<Row, Integer>) record -> 0) //key is required for stateful count (limit 100 impl)
      .map(new LimitMapper());

    // WRITE d_year|i_brand_id|i_brand|sum_agg
    // parallelism is 1 because of keyBy(0) so it does not mess the order up
    output.sinkTo(FileSink.forRowFormat(new Path(pathResults), new Encoder<Row>() {

      @Override public void encode(Row row, OutputStream outputStream)
        throws IOException {
        String output = row.getField(1) + FIELD_DELIMITER + row.getField(7) + FIELD_DELIMITER + row.getField(
          8) + FIELD_DELIMITER + row.getField(10);
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
    return row -> String.valueOf(row.getField(1)) + String.valueOf(row.getField(8)) + String.valueOf(row.getField(7));
  }

  private static class JoinRows
    extends KeyedCoProcessFunction<Integer, Row, Row, Row> {

    //    The state of Row belonging to dataStream 1
    private MapState<Integer, Row> state1;
    //    The state of Row belonging to dataStream 2
    private MapState<Integer, Row> state2;

    @Override
    public void open(Configuration parameters) {
      state1 = getRuntimeContext().getMapState(
        new MapStateDescriptor<>("rows_dataStream_1", Integer.class, Row.class));
      state2 = getRuntimeContext().getMapState(
        new MapStateDescriptor<>("rows_dataStream_2", Integer.class, Row.class));
    }

    private Row stateJoin(Row currentRow, int currentDatastream, Context context) throws Exception {
      final Integer currentKey = context.getCurrentKey();
      MapState<Integer, Row> myState = currentDatastream == 1 ? state1 : state2;
      MapState<Integer, Row> otherState = currentDatastream == 1 ? state2 : state1;
      // join with the other datastream by looking into the state of the other datastream
      final Row otherRow = otherState.get(currentKey);
      if (otherRow == null) { // did not find a row to join with, store the row for later join
        myState.put(currentKey, currentRow);
        return null;
      } else { // found a row to join with (same key) so do the join
        return currentDatastream == 1 ? Row.join(currentRow, otherRow) : Row.join(otherRow, currentRow);
      }
    }

    @Override
    public void processElement1(Row currentRow, Context context, Collector<Row> collector)
        throws Exception {
      final Row jointRow = stateJoin(currentRow, 1, context);
      if (jointRow != null) {
        collector.collect(jointRow);
      }
    }

    @Override
    public void processElement2(Row currentRow, Context context, Collector<Row> collector)
        throws Exception {
      final Row jointRow = stateJoin(currentRow, 2, context);
      if (jointRow != null) {
        collector.collect(jointRow);
      }
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

