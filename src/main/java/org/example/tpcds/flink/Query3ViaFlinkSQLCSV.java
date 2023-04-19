package org.example.tpcds.flink;

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

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.api.internal.TableEnvironmentInternal;
import org.apache.flink.table.catalog.ConnectorCatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.table.types.utils.TypeConversions;

import org.apache.flink.shaded.guava30.com.google.common.base.Strings;

import org.example.tpcds.flink.csvSchemas.csvSchemas.TpcdsSchema;
import org.example.tpcds.flink.csvSchemas.csvSchemas.TpcdsSchemaProvider;

import java.util.HashMap;
import java.util.Map;


public class Query3ViaFlinkSQLCSV {

  private static final String COL_DELIMITER = "|";

  public static void main(String[] args) throws Exception {
    System.out.print("This pipeline does not work with globs: https://issues.apache.org/jira/browse/FLINK-6417 , workaround with filter does not work on S3 filesystem and FileInputFormat.setFilePaths is not supported");
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

    // init Table Env
    EnvironmentSettings environmentSettings =
      EnvironmentSettings.newInstance().inBatchMode().build();
    TableEnvironment tableEnv = TableEnvironment.create(environmentSettings);

    // config Optimizer parameters
    tableEnv.getConfig()
      .getConfiguration()
      .setInteger(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, Integer.parseInt(parallelism));
    tableEnv.getConfig()
      .getConfiguration()
      .setLong(OptimizerConfigOptions.TABLE_OPTIMIZER_BROADCAST_JOIN_THRESHOLD, 10 * 1024 * 1024);
    tableEnv.getConfig()
      .getConfiguration()
      .setBoolean(OptimizerConfigOptions.TABLE_OPTIMIZER_JOIN_REORDER_ENABLED, true);

    // register TPC-DS tables
    registerTable("date_dim", pathDateDim, tableEnv);
    registerTable("item", pathItem, tableEnv);
    registerTable("store_sales", pathStoreSales, tableEnv);

    String query =
      "SELECT dt.d_year, item.i_brand_id brand_id, item.i_brand brand,SUM(ss_ext_sales_price) sum_agg"
        + " FROM  date_dim dt, store_sales, item"
        + " WHERE dt.d_date_sk = store_sales.ss_sold_date_sk"
        + "   AND store_sales.ss_item_sk = item.i_item_sk" + "   AND item.i_manufact_id = 128"
        + "   AND dt.d_moy = 11" + " GROUP BY dt.d_year, item.i_brand, item.i_brand_id"
        + " ORDER BY dt.d_year, sum_agg desc, brand_id" + " LIMIT 100";

    Table resultTable = tableEnv.sqlQuery(query);

    // register sink table
    final String sinkTableName = "sinkTable";
    ((TableEnvironmentInternal) tableEnv)
      .registerTableSinkInternal(sinkTableName,
        new CsvTableSink(
          pathResults,
          "|",
          1,
          FileSystem.WriteMode.OVERWRITE,
          resultTable.getSchema().getFieldNames(),
          resultTable.getSchema().getFieldDataTypes())
      );

    System.out.print("TPC-DS Query 3 Flink SQL CSV - start");

    final long start = System.currentTimeMillis();
    TableResult tableResult = resultTable.executeInsert(sinkTableName);
    // wait job finish
    tableResult.getJobClient().get().getJobExecutionResult().get();

    final long end = System.currentTimeMillis();
    final long runTime = (end - start) / 1000;
    System.out.printf(
      "TPC-DS %s - end - %d m %d s. Total: %d%n", "Query 3 ", (runTime / 60), (runTime % 60), runTime);
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

  private static void registerTable(String tableName, String filePath, TableEnvironment tableEnv) {
    TpcdsSchema schema = TpcdsSchemaProvider.getTableSchema(tableName);
    CsvTableSource.Builder builder = CsvTableSource.builder();
    builder.path(filePath);
    for (int i = 0; i < schema.getFieldNames().size(); i++) {
      builder.field(schema.getFieldNames().get(i),
        TypeConversions.fromDataTypeToLegacyInfo(schema.getFieldTypes().get(i)));
    }
    builder.fieldDelimiter(COL_DELIMITER);
    builder.emptyColumnAsNull();
    builder.lineDelimiter("\n");
    CsvTableSource tableSource = builder.build();
    ConnectorCatalogTable catalogTable = ConnectorCatalogTable.source(tableSource, true);
    tableEnv.getCatalog(tableEnv.getCurrentCatalog()).ifPresent(catalog -> {
      try {
        catalog.createTable(new ObjectPath(tableEnv.getCurrentDatabase(), tableName), catalogTable,
          false);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
  }
}
