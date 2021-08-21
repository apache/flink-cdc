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

package com.ververica.cdc.debezium;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.HashMap;

/**
 * A debezium-json message format implementation of {@link DebeziumDeserializationSchema} which converts the received
 * {@link SourceRecord} into JsonString.
 *
 * {
 *     "schema": {
 *         "type": "struct",
 *         "fields": [
 *             {
 *                 "type": "struct",
 *                 "fields": [
 *                     {
 *                         "type": "int32",
 *                         "optional": false,
 *                         "field": "product_id"
 *                     },
 *                     {
 *                         "type": "int32",
 *                         "optional": false,
 *                         "field": "quantity"
 *                     }
 *                 ],
 *                 "optional": true,
 *                 "name": "mysql_binlog_source.inventory_1nrc6up.products_on_hand.Value",
 *                 "field": "before"
 *             },
 *             {
 *                 "type": "struct",
 *                 "fields": [
 *                     {
 *                         "type": "int32",
 *                         "optional": false,
 *                         "field": "product_id"
 *                     },
 *                     {
 *                         "type": "int32",
 *                         "optional": false,
 *                         "field": "quantity"
 *                     }
 *                 ],
 *                 "optional": true,
 *                 "name": "mysql_binlog_source.inventory_1nrc6up.products_on_hand.Value",
 *                 "field": "after"
 *             },
 *             {
 *                 "type": "struct",
 *                 "fields": [
 *                     {
 *                         "type": "string",
 *                         "optional": false,
 *                         "field": "version"
 *                     },
 *                     {
 *                         "type": "string",
 *                         "optional": false,
 *                         "field": "connector"
 *                     },
 *                     {
 *                         "type": "string",
 *                         "optional": false,
 *                         "field": "name"
 *                     },
 *                     {
 *                         "type": "int64",
 *                         "optional": false,
 *                         "field": "ts_ms"
 *                     },
 *                     {
 *                         "type": "string",
 *                         "optional": true,
 *                         "name": "io.debezium.data.Enum",
 *                         "version": 1,
 *                         "parameters": {
 *                             "allowed": "true,last,false"
 *                         },
 *                         "default": "false",
 *                         "field": "snapshot"
 *                     },
 *                     {
 *                         "type": "string",
 *                         "optional": false,
 *                         "field": "db"
 *                     },
 *                     {
 *                         "type": "string",
 *                         "optional": true,
 *                         "field": "sequence"
 *                     },
 *                     {
 *                         "type": "string",
 *                         "optional": true,
 *                         "field": "table"
 *                     },
 *                     {
 *                         "type": "int64",
 *                         "optional": false,
 *                         "field": "server_id"
 *                     },
 *                     {
 *                         "type": "string",
 *                         "optional": true,
 *                         "field": "gtid"
 *                     },
 *                     {
 *                         "type": "string",
 *                         "optional": false,
 *                         "field": "file"
 *                     },
 *                     {
 *                         "type": "int64",
 *                         "optional": false,
 *                         "field": "pos"
 *                     },
 *                     {
 *                         "type": "int32",
 *                         "optional": false,
 *                         "field": "row"
 *                     },
 *                     {
 *                         "type": "int64",
 *                         "optional": true,
 *                         "field": "thread"
 *                     },
 *                     {
 *                         "type": "string",
 *                         "optional": true,
 *                         "field": "query"
 *                     }
 *                 ],
 *                 "optional": false,
 *                 "name": "io.debezium.connector.mysql.Source",
 *                 "field": "source"
 *             },
 *             {
 *                 "type": "string",
 *                 "optional": false,
 *                 "field": "op"
 *             },
 *             {
 *                 "type": "int64",
 *                 "optional": true,
 *                 "field": "ts_ms"
 *             },
 *             {
 *                 "type": "struct",
 *                 "fields": [
 *                     {
 *                         "type": "string",
 *                         "optional": false,
 *                         "field": "id"
 *                     },
 *                     {
 *                         "type": "int64",
 *                         "optional": false,
 *                         "field": "total_order"
 *                     },
 *                     {
 *                         "type": "int64",
 *                         "optional": false,
 *                         "field": "data_collection_order"
 *                     }
 *                 ],
 *                 "optional": true,
 *                 "field": "transaction"
 *             }
 *         ],
 *         "optional": false,
 *         "name": "mysql_binlog_source.inventory_1nrc6up.products_on_hand.Envelope"
 *     },
 *     "payload": {
 *         "before": null,
 *         "after": {
 *             "product_id": 109,
 *             "quantity": 5
 *         },
 *         "source": {
 *             "version": "1.5.2.Final",
 *             "connector": "mysql",
 *             "name": "mysql_binlog_source",
 *             "ts_ms": 1629522088706,
 *             "snapshot": "last",
 *             "db": "inventory_1nrc6up",
 *             "sequence": null,
 *             "table": "products_on_hand",
 *             "server_id": 0,
 *             "gtid": null,
 *             "file": "mysql-bin.000003",
 *             "pos": 6537,
 *             "row": 0,
 *             "thread": null,
 *             "query": null
 *         },
 *         "op": "r",
 *         "ts_ms": 1629522088706,
 *         "transaction": null
 *     }
 * }
 */
public class JsonDebeziumDeserializationSchema implements DebeziumDeserializationSchema<String> {
    static final JsonConverter converter = new JsonConverter();

    public JsonDebeziumDeserializationSchema() {
        HashMap<String, Object> map = new HashMap<>();
        converter.configure(map, true);
    }

    @Override
    public void deserialize(SourceRecord record, Collector<String> out) throws Exception {
        byte[] bytes = converter.fromConnectData(record.topic(), record.valueSchema(), record.value());
        out.collect(new String(bytes));
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}
