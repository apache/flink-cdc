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

package org.apache.flink.cdc.connectors.kafka.source;

import org.apache.flink.annotation.Internal;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.source.MetadataAccessor;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;

/**
 * A {@link MetadataAccessor} for Kafka source. Kafka does not maintain table schemas, so all
 * methods return empty results or throw {@link UnsupportedOperationException} for schema access.
 */
@Internal
public class KafkaMetadataAccessor implements MetadataAccessor {

    private final String topic;

    public KafkaMetadataAccessor(String topic) {
        this.topic = topic;
    }

    @Override
    public List<String> listNamespaces() {
        return Collections.emptyList();
    }

    @Override
    public List<String> listSchemas(@Nullable String namespace) {
        return Collections.emptyList();
    }

    @Override
    public List<TableId> listTables(@Nullable String namespace, @Nullable String schemaName) {
        // Return the topic as a table
        return Collections.singletonList(TableId.tableId(topic));
    }

    @Override
    public Schema getTableSchema(TableId tableId) {
        throw new UnsupportedOperationException(
                "Kafka source does not support accessing table schema from external system. "
                        + "The schema is inferred from the Canal JSON messages.");
    }
}
