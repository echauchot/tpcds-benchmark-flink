# tpcds-benchmark-flink
TPCDS queries implementation over Flink APIs

## Get the input data
To run the examples, you need 3 input files `date_dim.dat`, `store_sales.dat` and `item.dat`.
For size reasons, they are not added to the github repository, you need to generate them as CSV 
with TPCDS tools 

### generate the input data with tpcds tools
You can use the Databricks' forked version of [tpcds-kit](https://github.com/databricks/tpcds-kit) and their `dsdgen` tool for data generation.

### copy the generated files at the expected place
Copy the 3 generated files to `src/test/resources` in the project directory.

## Compile
`mvn clean package`

## Run the DataSet example
Example on a local flink with 4 parallel threads, the output file is _target/query3_dataset.csv_ :
`java -cp "target/*"  org.example.tpcds.flink.Query3ViaFlinkRowDataset --pathDateDim="src/test/resources/date_dim.dat" --pathStoreSales="src/test/resources/store_sales.dat" --pathItem="src/test/resources/item.dat" --pathResults="target/query3_dataset.csv" --flinkMaster="[local]" --parallelism=4`


## Run the DataStream example
Example on a local flink with 4 parallel threads, the output directoy is _target/query3_datastream.csv_ :
`java -cp "target/*"  org.example.tpcds.flink.Query3ViaFlinkRowDatastream --pathDateDim="src/test/resources/date_dim.dat" --pathStoreSales="src/test/resources/store_sales.dat" --pathItem="src/test/resources/item.dat" --pathResults="target/query3_datastream.csv" --flinkMaster="[local]" --parallelism=4`

## Run the SQL example
Example on a local flink with 4 parallel threads, the output file is _target/query3_sql.csv_ :
`java -cp "target/*"  org.example.tpcds.flink.Query3ViaFlinkSQL --pathDateDim="src/test/resources/date_dim.dat" --pathStoreSales="src/test/resources/store_sales.dat" --pathItem="src/test/resources/item.dat" --pathResults="target/query3_sql.csv" --flinkMaster="[local]" --parallelism=4`
