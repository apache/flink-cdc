/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.connectors.hudi.sink.util;

import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.types.logical.RowType;

import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.exception.HoodieValidationException;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.util.AvroSchemaConverter;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/** Utils for writer configurations. */
public class ConfigUtils {
    public static Configuration createTableConfig(
            Configuration baseConfig, Schema cdcSchema, TableId tableId) {
        Configuration localTableConfig = new Configuration(baseConfig);
        String rootPath = baseConfig.get(FlinkOptions.PATH);
        String tableBasePath =
                String.format(
                        "%s/%s/%s", rootPath, tableId.getSchemaName(), tableId.getTableName());
        localTableConfig.set(FlinkOptions.PATH, tableBasePath);
        localTableConfig.set(FlinkOptions.DATABASE_NAME, tableId.getSchemaName());
        localTableConfig.set(FlinkOptions.TABLE_NAME, tableId.getTableName());

        RowType rowType = RowDataUtils.toRowType(cdcSchema);
        String tableAvroSchema = AvroSchemaConverter.convertToSchema(rowType).toString();
        localTableConfig.set(FlinkOptions.SOURCE_AVRO_SCHEMA, tableAvroSchema);
        // set up key options
        ConfigUtils.setupHoodieKeyOptions(localTableConfig, cdcSchema);

        return localTableConfig;
    }

    /**
     * Sets up the hoodie key options (e.g. record key and partition key) from the table definition.
     */
    private static void setupHoodieKeyOptions(Configuration conf, Schema schema) {
        List<String> pkColumns = schema.primaryKeys();
        if (!pkColumns.isEmpty()) {
            // the PRIMARY KEY syntax always has higher priority than option
            // FlinkOptions#RECORD_KEY_FIELD
            String recordKey = String.join(",", pkColumns);
            conf.set(FlinkOptions.RECORD_KEY_FIELD, recordKey);
        }

        List<String> partitionKeys = schema.partitionKeys();
        if (!partitionKeys.isEmpty()) {
            // the PARTITIONED BY syntax always has higher priority than option
            // FlinkOptions#PARTITION_PATH_FIELD
            conf.set(FlinkOptions.PARTITION_PATH_FIELD, String.join(",", partitionKeys));
        }

        for (Map.Entry<String, String> kv : schema.options().entrySet()) {
            conf.setString(kv.getKey(), kv.getValue());
        }

        if (conf.get(FlinkOptions.INDEX_TYPE).equals(HoodieIndex.IndexType.BUCKET.name())) {
            if (conf.get(FlinkOptions.INDEX_KEY_FIELD).isEmpty()) {
                conf.set(FlinkOptions.INDEX_KEY_FIELD, conf.get(FlinkOptions.RECORD_KEY_FIELD));
            } else {
                Set<String> recordKeySet =
                        Arrays.stream(conf.get(FlinkOptions.RECORD_KEY_FIELD).split(","))
                                .collect(Collectors.toSet());
                Set<String> indexKeySet =
                        Arrays.stream(conf.get(FlinkOptions.INDEX_KEY_FIELD).split(","))
                                .collect(Collectors.toSet());
                if (!recordKeySet.containsAll(indexKeySet)) {
                    throw new HoodieValidationException(
                            FlinkOptions.INDEX_KEY_FIELD
                                    + " should be a subset of or equal to the recordKey fields");
                }
            }
        }
    }

    /**
     * Sets up the hoodie key options (e.g. record key and partition key) from the table definition.
     */
    public static void setupHoodieKeyOptions(Map<String, String> tableOptions, Schema schema) {
        List<String> pkColumns = schema.primaryKeys();
        if (!pkColumns.isEmpty()) {
            // the PRIMARY KEY syntax always has higher priority than option
            // FlinkOptions#RECORD_KEY_FIELD
            String recordKey = String.join(",", pkColumns);
            tableOptions.put(FlinkOptions.RECORD_KEY_FIELD.key(), recordKey);
        }

        List<String> partitionKeys = schema.partitionKeys();
        if (!partitionKeys.isEmpty()) {
            // the PARTITIONED BY syntax always has higher priority than option
            // FlinkOptions#PARTITION_PATH_FIELD
            tableOptions.put(
                    FlinkOptions.PARTITION_PATH_FIELD.key(), String.join(",", partitionKeys));
        }

        // setup ordering fields from schema options
        tableOptions.putAll(schema.options());
    }
}
