package org.example.tpcds.flink.flink;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.shaded.guava30.com.google.common.base.Strings;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.parquet.schema.MessageType;

// SELECT dt.d_year, item.i_brand_id brand_id, item.i_brand brand,SUM(ss_ext_sales_price) sum_agg
// FROM  date_dim dt, store_sales, item
// WHERE dt.d_date_sk = store_sales.ss_sold_date_sk
//   AND store_sales.ss_item_sk = item.i_item_sk
//   AND item.i_manufact_id = 128
//   AND dt.d_moy=11
// GROUP BY dt.d_year, item.i_brand, item.i_brand_id
// ORDER BY dt.d_year, sum_agg desc, brand_id
// LIMIT 100

public class Query3ViaFlinkAvroDataStream {
/*
  private static final Logger LOG = LogManager.getLogger(Query3ViaFlinkAvroDataStream.class);

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
    if ("[local]".equals(flinkMaster)) {
      env = StreamExecutionEnvironment.createLocalEnvironment();
    } else { // use [auto] that works for cluster execution as well
      env = StreamExecutionEnvironment.getExecutionEnvironment();
    }
    env.setRuntimeMode(RuntimeExecutionMode.BATCH);
    env.setParallelism(Integer.parseInt(parallelism));
    env.getConfig().enableForceAvro();
    env.getConfig().enableObjectReuse();

    // Table date_dim
    final ParquetAvroInputFormat dateDimInputFormat = createInputFormat("date_dim", pathDateDim);
    // SELECT d_moy, d_date_sk, d_year
    String[] projectedFieldNames = new String[] { "d_moy", "d_date_sk", "d_year" };
    dateDimInputFormat.selectFields(projectedFieldNames);
    final Schema dateDimAvroSchema = dateDimInputFormat.getAvroSchema();
    final DataStream<GenericRecord> dateDim = env
      .createInput(dateDimInputFormat, new GenericRecordAvroTypeInfo(dateDimAvroSchema))
      // WHERE dt.d_moy=11
      // filter as parquet predicateFilter does not work  https://issues.apache.org/jira/browse/FLINK-21520
      .filter((FilterFunction<GenericRecord>) value -> value.get("d_moy") != null && (
        (Integer) value.get("d_moy") == 11) && (value.get("d_date_sk") != null));

    // Table store_sales
    final ParquetAvroInputFormat storeSalesInputFormat = createInputFormat("store_sales",
      pathStoreSales);
    // SELECT ss_sold_date_sk, ss_item_sk, ss_ext_sales_price
    projectedFieldNames = new String[] { "ss_sold_date_sk", "ss_item_sk", "ss_ext_sales_price" };
    storeSalesInputFormat.selectFields(projectedFieldNames);
    final Schema storeSalesAvroSchema = storeSalesInputFormat.getAvroSchema();
    final DataStream<GenericRecord> storeSales = env
      .createInput(storeSalesInputFormat, new GenericRecordAvroTypeInfo(storeSalesAvroSchema))
      .filter(
        // filter as parquet predicateFilter does not work  https://issues.apache.org/jira/browse/FLINK-21520
        (FilterFunction<GenericRecord>) value -> value.get("ss_sold_date_sk") != null
          && value.get("ss_item_sk") != null);

    // Table item
    final ParquetAvroInputFormat itemInputFormat = createInputFormat("item", pathItem);
    // SELECT i_item_sk, i_brand_id, i_brand, i_manufact_id
    projectedFieldNames = new String[] { "i_item_sk", "i_brand_id", "i_brand", "i_manufact_id" };
    itemInputFormat.selectFields(projectedFieldNames);
    final Schema itemAvroSchema = itemInputFormat.getAvroSchema();
    final DataStream<GenericRecord> item = env
      .createInput(itemInputFormat, new GenericRecordAvroTypeInfo(itemAvroSchema))
      // WHERE item.i_manufact_id = 128
      // filter as parquet predicateFilter does not work https://issues.apache.org/jira/browse/FLINK-21520
      .filter((FilterFunction<GenericRecord>) value -> value.get("i_manufact_id") != null
        && (Integer) value.get("i_manufact_id") == 128 && value.get("i_item_sk") != null);

    // Join1: WHERE dt.d_date_sk = store_sales.ss_sold_date_sk
    Schema schemaJoinDateSk = AvroUtils
      .getSchemaMerged(dateDimAvroSchema, storeSalesAvroSchema, "recordsJoinDateSk");
    final DataStream<GenericRecord> recordsJoinDateSk = dateDim
      .keyBy((KeySelector<GenericRecord, Integer>) value -> (Integer) value.get("d_date_sk"))
      .connect(storeSales.keyBy(
        (KeySelector<GenericRecord, Integer>) value -> (Integer) value.get("ss_sold_date_sk")))
      .process(new JoinRecords(dateDimAvroSchema, storeSalesAvroSchema, schemaJoinDateSk))
      .returns(new GenericRecordAvroTypeInfo(schemaJoinDateSk));

    // Join2: WHERE store_sales.ss_item_sk = item.i_item_sk
    Schema schemaJoinItemSk = getSchemaJoinItemSk(itemAvroSchema, schemaJoinDateSk);
    final DataStream<GenericRecord> recordsJoinItemSk = recordsJoinDateSk
      .keyBy((KeySelector<GenericRecord, Integer>) value -> (Integer) value.get("ss_item_sk"))
      .connect(
        item.keyBy((KeySelector<GenericRecord, Integer>) value -> (Integer) value.get("i_item_sk")))
      .process(new JoinRecords(schemaJoinDateSk, itemAvroSchema, schemaJoinItemSk))
      .returns(new GenericRecordAvroTypeInfo(schemaJoinItemSk));

    // GROUP BY dt.d_year, item.i_brand, item.i_brand_id
    final DataStream<GenericRecord> sum = recordsJoinItemSk
      .keyBy(new KeySelector<GenericRecord, String>() {

        @Override public String getKey(GenericRecord value) throws Exception {
          final StringBuilder stringBuilder = new StringBuilder();
          stringBuilder.append(value.get("d_year")).append(value.get("i_brand"))
            .append(value.get("i_brand_id"));
          return stringBuilder.toString();
        }
      })
      // SUM(ss_ext_sales_price) sum_agg
      .reduce(new ReduceFunction<GenericRecord>() {

        @Override public GenericRecord reduce(GenericRecord record1, GenericRecord record2)
          throws Exception {
          GenericRecord output = record1;
          final Integer record1Price = (Integer) record1.get("ss_ext_sales_price");
          final Integer record2Price = (Integer) record2.get("ss_ext_sales_price");
          if (record1Price != null && record2Price != null) {
            output.put("sum_agg", record1Price + record2Price);
          } else if (record1Price == null) {
            output.put("sum_agg", record2Price);
          } else {
            output.put("sum_agg", record1Price);
          }
          return output;
        }
      }).returns(new GenericRecordAvroTypeInfo(schemaJoinItemSk));

    // TODO this manual oder seems to not be working but the aim was to workaround the not working DataStream#join() sum DataStream, is correct so the POC is done
    final SingleOutputStreamOperator<GenericRecord> output = sum
      .windowAll(GlobalWindows.create())
      // ORDER BY dt.d_year, sum_agg desc, brand_id
      .process(new ProcessAllWindowFunction<GenericRecord, GenericRecord, GlobalWindow>() {

        @Override public void process(Context context, Iterable<GenericRecord> records,
          Collector<GenericRecord> collector) throws Exception {
          List<GenericRecord> output = new ArrayList<>();
          for (GenericRecord record : records){
            output.add(record);
          }
          output.sort(new OrderComparator());
          for (GenericRecord record : output){
            collector.collect(record);
          }
        }
      })
      .returns(new GenericRecordAvroTypeInfo(schemaJoinItemSk))
      // LIMIT 100
      .keyBy((KeySelector<GenericRecord, Integer>) record -> 0)// key is required for stateful count (limit 100 impl), use fake 0 key
      .map(new LimitMapper());


    // parallelism is 1 so it does not mess the order up
    output.addSink(StreamingFileSink.forRowFormat(new Path(pathResults), new Encoder<GenericRecord>() {

      @Override public void encode(GenericRecord record, OutputStream outputStream)
        throws IOException {
        String output = new StringBuilder().append(record.get("d_year")).append("|")
          .append(record.get("i_brand_id")).append("|").append(record.get("i_brand")).append("|")
          .append(record.get("sum_agg")).toString();
        outputStream.write(output.getBytes(StandardCharsets.UTF_8));
        outputStream.write(10);
      }
    }).build());

    LOG.info("TPC-DS Query 3 Flink DataStream - start");
    final long start = System.currentTimeMillis();
    env.execute();
    final long end = System.currentTimeMillis();
    final long runTime = (end - start) / 1000;
    LOG.info("TPC-DS {} - end - {}m {}s. Total: {}", "Query 3", (runTime / 60), (runTime % 60),
      runTime);
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

  private static Schema getSchemaJoinItemSk(Schema schemaItem, Schema schemaJoinDateSk) {
    Schema schemaJoinItemSk = AvroUtils
      .getSchemaMerged(schemaJoinDateSk, schemaItem, "recordsJoinItemSk");

    List<Schema.Field> mergedFields = new ArrayList<>();
    for (Schema.Field f : schemaJoinItemSk.getFields()) {
      mergedFields.add(new Schema.Field(f.name(), f.schema(), f.doc(), f.defaultVal()));
    }
    Schema.Field f = schemaJoinItemSk.getField("ss_ext_sales_price");
    mergedFields.add(new Schema.Field("sum_agg", f.schema(), f.doc(), f.defaultVal()));

    Schema schemaJoin = Schema.createRecord("recordsJoinItemSk", null, null, false);
    schemaJoin.setFields(mergedFields);

    return schemaJoin;
  }
  private static class OrderComparator implements Comparator<GenericRecord> {

    @Override
    public int compare(GenericRecord a, GenericRecord b) {
      //ORDER BY dt.d_year, sum_agg desc, brand_id
      int aDYear = (int) a.get("d_year");
      int bDYear = (int) b.get("d_year");
      if (bDYear != aDYear) {
        return aDYear - bDYear;
      }

      int aSumAgg = (int) a.get("sum_agg");
      int bSumAgg = (int) b.get("sum_agg");
      if (bSumAgg != aSumAgg) {
        return bSumAgg - aSumAgg;
      }

      int aIBrandId = (int) a.get("i_brand_id");
      int bIBrandId = (int) b.get("i_brand_id");
      return aIBrandId - bIBrandId;
    }
  }

  private static ParquetAvroInputFormat createInputFormat(String tableName, String filePath)
    throws Exception {
    final MessageType parquetSchema = BenchmarkHelper.getParquetSchema(tableName);

    final ParquetAvroInputFormat parquetAvroInputFormat = new ParquetAvroInputFormat(
      new Path(filePath), parquetSchema);
    return parquetAvroInputFormat;
  }

  private static class JoinRecords
    extends KeyedCoProcessFunction<Integer, GenericRecord, GenericRecord, GenericRecord> {

    private final String schemaLhsString;
    private final String schemaRhsString;
    private final String schemaString;
    private transient Schema schemaLhs;
    private transient Schema schemaRhs;
    private transient Schema outputSchema;

    //    The state of GenericRecords belonging to dataStream 1
    private MapState<Integer, GenericRecord> state1;
    //    The state of GenericRecords belonging to dataStream 2
    private MapState<Integer, GenericRecord> state2;

    public JoinRecords(Schema schemaLhs, Schema schemaRhs, Schema outputSchema) {
      this.schemaLhs = schemaLhs;
      this.schemaRhs = schemaRhs;
      this.outputSchema = outputSchema;
      this.schemaLhsString = schemaLhs.toString();
      this.schemaRhsString = schemaRhs.toString();
      this.schemaString = outputSchema.toString();
    }

    @Override public void open(Configuration parameters) throws Exception {
      state1 = getRuntimeContext().getMapState(
        new MapStateDescriptor<>("records_dataStream_1", Integer.class, GenericRecord.class));
      state2 = getRuntimeContext().getMapState(
        new MapStateDescriptor<>("records_dataStream_2", Integer.class, GenericRecord.class));
    }

    private GenericRecord joinRecords(GenericRecord first, GenericRecord second)
      throws Exception {
      // after deserialization
      if (schemaLhs == null) {
        schemaLhs = new Schema.Parser().parse(schemaLhsString);
      }
      if (schemaRhs == null) {
        schemaRhs = new Schema.Parser().parse(schemaRhsString);
      }
      if (outputSchema == null) {
        outputSchema = new Schema.Parser().parse(schemaString);
      }

      GenericRecord outputRecord = new GenericRecordBuilder(outputSchema).build();
      for (Schema.Field f : outputSchema.getFields()) {
        if (schemaLhs.getField(f.name()) != null) {
          outputRecord.put(f.name(), first.get(f.name()));
        } else if (schemaRhs.getField(f.name()) != null) {
          outputRecord.put(f.name(), second.get(f.name()));
        }
      }
      return outputRecord;
    }

    private GenericRecord stateJoin(GenericRecord currentRecord, int currentDatastream, Context context) throws Exception {
      final Integer currentKey = context.getCurrentKey();
      MapState<Integer, GenericRecord> myState = currentDatastream == 1 ? state1 : state2;
      MapState<Integer, GenericRecord> otherState = currentDatastream == 1 ? state2 : state1;
      // join with the other datastream by looking into the state of the other datastream
      final GenericRecord otherRecord = otherState.get(currentKey);
      if (otherRecord == null) { // did not find a record to join with, store record for later join
        myState.put(currentKey, currentRecord);
        return null;
      } else { // found a record to join with (same key), join (with using the correct avro schema)
        return currentDatastream == 1 ? joinRecords(currentRecord, otherRecord) : joinRecords(otherRecord, currentRecord);
      }
    }

    @Override public void processElement1(GenericRecord currentRecord, Context context,
      Collector<GenericRecord> collector) throws Exception {
      final GenericRecord jointRecord = stateJoin(currentRecord, 1, context);
      if (jointRecord != null) {
        collector.collect(jointRecord);
      }
    }

    @Override public void processElement2(GenericRecord currentRecord, Context context,
      Collector<GenericRecord> collector) throws Exception {
      final GenericRecord jointRecord = stateJoin(currentRecord, 2, context);
      if (jointRecord != null) {
        collector.collect(jointRecord);
      }
    }
  }

  private static class LimitMapper extends RichMapFunction<GenericRecord, GenericRecord> {

    ValueState<Integer> state;

    @Override public void open(Configuration parameters) throws Exception {
      state = getRuntimeContext().getState(new ValueStateDescriptor<>("countState", Integer.class));
    }

    @Override public GenericRecord map(GenericRecord record) throws Exception {
      Integer count = state.value();
      if (count == null){
        count = 0;
      }
      state.update(++count);
      if (count > 100){
        close();
      }
      return record;
    }
  }
*/
}
