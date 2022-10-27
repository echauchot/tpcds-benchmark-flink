/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.example.tpcds.flink.flink.csvSchemas;

import java.sql.Date;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;

/**
 * Class to provide all TPC-DS tables' schema information. The data type of column use {@link
 * DataType}
 */
public class TpcdsSchemaProvider {

    private static final int tpcdsTableNums = 24;
    private static final Map<String, TpcdsSchema> schemaMap = createTableSchemas();

    private static Map<String, TpcdsSchema> createTableSchemas() {
        final Map<String, TpcdsSchema> schemaMap = new HashMap<>(tpcdsTableNums);
        schemaMap.put(
                "store_sales",
                new TpcdsSchema(
                        Arrays.asList(
                                new Column("ss_sold_date_sk", DataTypes.BIGINT()),
                                new Column("ss_sold_time_sk", DataTypes.BIGINT()),
                                new Column("ss_item_sk", DataTypes.BIGINT()),
                                new Column("ss_customer_sk", DataTypes.BIGINT()),
                                new Column("ss_cdemo_sk", DataTypes.BIGINT()),
                                new Column("ss_hdemo_sk", DataTypes.BIGINT()),
                                new Column("ss_addr_sk", DataTypes.BIGINT()),
                                new Column("ss_store_sk", DataTypes.BIGINT()),
                                new Column("ss_promo_sk", DataTypes.BIGINT()),
                                new Column("ss_ticket_number", DataTypes.BIGINT()),
                                new Column("ss_quantity", DataTypes.INT()),
                                new Column("ss_wholesale_cost", DataTypes.DECIMAL(7, 2)),
                                new Column("ss_list_price", DataTypes.DECIMAL(7, 2)),
                                new Column("ss_sales_price", DataTypes.DECIMAL(7, 2)),
                                new Column("ss_ext_discount_amt", DataTypes.DECIMAL(7, 2)),
                                new Column("ss_ext_sales_price", DataTypes.DECIMAL(7, 2)),
                                new Column("ss_ext_wholesale_cost", DataTypes.DECIMAL(7, 2)),
                                new Column("ss_ext_list_price", DataTypes.DECIMAL(7, 2)),
                                new Column("ss_ext_tax", DataTypes.DECIMAL(7, 2)),
                                new Column("ss_coupon_amt", DataTypes.DECIMAL(7, 2)),
                                new Column("ss_net_paid", DataTypes.DECIMAL(7, 2)),
                                new Column("ss_net_paid_inc_tax", DataTypes.DECIMAL(7, 2)),
                                new Column("ss_net_profit", DataTypes.DECIMAL(7, 2)))));
        schemaMap.put(
                "date_dim",
                new TpcdsSchema(
                        Arrays.asList(
                                new Column("d_date_sk", DataTypes.BIGINT()),
                                new Column("d_date_id", DataTypes.STRING()),
                                new Column("d_date", DataTypes.DATE().bridgedTo(Date.class)),
                                new Column("d_month_seq", DataTypes.INT()),
                                new Column("d_week_seq", DataTypes.INT()),
                                new Column("d_quarter_seq", DataTypes.INT()),
                                new Column("d_year", DataTypes.INT()),
                                new Column("d_dow", DataTypes.INT()),
                                new Column("d_moy", DataTypes.INT()),
                                new Column("d_dom", DataTypes.INT()),
                                new Column("d_qoy", DataTypes.INT()),
                                new Column("d_fy_year", DataTypes.INT()),
                                new Column("d_fy_quarter_seq", DataTypes.INT()),
                                new Column("d_fy_week_seq", DataTypes.INT()),
                                new Column("d_day_name", DataTypes.STRING()),
                                new Column("d_quarter_name", DataTypes.STRING()),
                                new Column("d_holiday", DataTypes.STRING()),
                                new Column("d_weekend", DataTypes.STRING()),
                                new Column("d_following_holiday", DataTypes.STRING()),
                                new Column("d_first_dom", DataTypes.INT()),
                                new Column("d_last_dom", DataTypes.INT()),
                                new Column("d_same_day_ly", DataTypes.INT()),
                                new Column("d_same_day_lq", DataTypes.INT()),
                                new Column("d_current_day", DataTypes.STRING()),
                                new Column("d_current_week", DataTypes.STRING()),
                                new Column("d_current_month", DataTypes.STRING()),
                                new Column("d_current_quarter", DataTypes.STRING()),
                                new Column("d_current_year", DataTypes.STRING()))));
        schemaMap.put(
                "item",
                new TpcdsSchema(
                        Arrays.asList(
                                new Column("i_item_sk", DataTypes.BIGINT()),
                                new Column("i_item_id", DataTypes.STRING()),
                                new Column(
                                        "i_rec_start_date", DataTypes.DATE().bridgedTo(Date.class)),
                                new Column(
                                        "i_rec_end_date", DataTypes.DATE().bridgedTo(Date.class)),
                                new Column("i_item_desc", DataTypes.STRING()),
                                new Column("i_current_price", DataTypes.DECIMAL(7, 2)),
                                new Column("i_wholesale_cost", DataTypes.DECIMAL(7, 2)),
                                new Column("i_brand_id", DataTypes.INT()),
                                new Column("i_brand", DataTypes.STRING()),
                                new Column("i_class_id", DataTypes.INT()),
                                new Column("i_class", DataTypes.STRING()),
                                new Column("i_category_id", DataTypes.INT()),
                                new Column("i_category", DataTypes.STRING()),
                                new Column("i_manufact_id", DataTypes.INT()),
                                new Column("i_manufact", DataTypes.STRING()),
                                new Column("i_size", DataTypes.STRING()),
                                new Column("i_formulation", DataTypes.STRING()),
                                new Column("i_color", DataTypes.STRING()),
                                new Column("i_units", DataTypes.STRING()),
                                new Column("i_container", DataTypes.STRING()),
                                new Column("i_manager_id", DataTypes.INT()),
                                new Column("i_product_name", DataTypes.STRING()))));
        return schemaMap;
    }

    public static TpcdsSchema getTableSchema(String tableName) {
        TpcdsSchema result = schemaMap.get(tableName);
        if (result != null) {
            return result;
        } else {
            throw new IllegalArgumentException(
                    "Table schema of table " + tableName + " does not exist.");
        }
    }
}
