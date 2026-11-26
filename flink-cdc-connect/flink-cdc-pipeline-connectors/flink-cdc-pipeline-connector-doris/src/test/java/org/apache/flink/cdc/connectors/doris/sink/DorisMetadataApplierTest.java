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

package org.apache.flink.cdc.connectors.doris.sink;

import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;

import org.apache.doris.flink.catalog.doris.DataModel;
import org.apache.doris.flink.catalog.doris.DorisSchemaFactory;
import org.apache.doris.flink.catalog.doris.TableSchema;
import org.apache.doris.flink.cfg.DorisOptions;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link DorisMetadataApplier}. */
class DorisMetadataApplierTest {

    @Test
    void testCreateTableKeepsNotNullConstraint() {
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.BIGINT().notNull())
                        .physicalColumn("name", DataTypes.STRING().notNull())
                        .physicalColumn("payload", DataTypes.STRING())
                        .primaryKey("id")
                        .build();
        DorisMetadataApplier applier =
                new DorisMetadataApplier(
                        DorisOptions.builder()
                                .setFenodes("127.0.0.1:8030")
                                .setUsername("root")
                                .build(),
                        new Configuration());

        TableSchema tableSchema = new TableSchema();
        tableSchema.setDatabase("test_db");
        tableSchema.setTable("test_table");
        tableSchema.setModel(DataModel.UNIQUE);
        tableSchema.setFields(applier.buildFields(schema));
        tableSchema.setKeys(Collections.singletonList("id"));
        tableSchema.setDistributeKeys(Collections.singletonList("id"));

        assertThat(DorisSchemaFactory.generateCreateTableDDL(tableSchema))
                .contains("`id` BIGINT NOT NULL")
                .contains("`name` STRING NOT NULL")
                .contains("`payload` STRING")
                .doesNotContain("`payload` STRING NOT NULL")
                .doesNotContain("NOT NULL NOT NULL");
    }
}
