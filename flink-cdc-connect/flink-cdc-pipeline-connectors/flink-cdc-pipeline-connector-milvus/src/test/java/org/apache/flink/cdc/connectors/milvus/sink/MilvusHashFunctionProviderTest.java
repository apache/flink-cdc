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

package org.apache.flink.cdc.connectors.milvus.sink;

import org.apache.flink.cdc.common.data.GenericArrayData;
import org.apache.flink.cdc.common.data.GenericRecordData;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.function.HashFunction;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;

/** Tests for {@link MilvusHashFunctionProvider}. */
class MilvusHashFunctionProviderTest {

    private static final TableId TABLE_ID = TableId.parse("inventory.docs");
    private static final Schema SCHEMA =
            Schema.newBuilder()
                    .physicalColumn("id", DataTypes.BIGINT().notNull())
                    .physicalColumn("title", DataTypes.STRING())
                    .physicalColumn("embedding", DataTypes.ARRAY(DataTypes.FLOAT()).notNull())
                    .primaryKey("id")
                    .build();

    @Test
    void testHashesSamePrimaryKeyToSameBucket() {
        HashFunction<DataChangeEvent> hashFunction =
                createHashFunction(MilvusTestUtils.defaultConfig());

        int insertHash =
                hashFunction.hashcode(DataChangeEvent.insertEvent(TABLE_ID, record(1L, "created")));
        int updateHash =
                hashFunction.hashcode(
                        DataChangeEvent.updateEvent(
                                TABLE_ID, record(1L, "created"), record(1L, "updated")));
        int deleteHash =
                hashFunction.hashcode(DataChangeEvent.deleteEvent(TABLE_ID, record(1L, "updated")));

        Assertions.assertThat(updateHash).isEqualTo(insertHash);
        Assertions.assertThat(deleteHash).isEqualTo(insertHash);
    }

    @Test
    void testHashesMappedCollectionName() {
        MilvusDataSinkConfig mappedConfig =
                MilvusTestUtils.config(
                        Collections.singletonMap(TABLE_ID, "docs_v2"),
                        MilvusTestUtils.defaultConfig().getVectorFields(),
                        Collections.emptyMap(),
                        100,
                        java.time.Duration.ofSeconds(10),
                        0,
                        java.time.Duration.ZERO,
                        true);

        HashFunction<DataChangeEvent> defaultHash =
                createHashFunction(MilvusTestUtils.defaultConfig());
        HashFunction<DataChangeEvent> mappedHash = createHashFunction(mappedConfig);

        Assertions.assertThat(
                        mappedHash.hashcode(
                                DataChangeEvent.insertEvent(TABLE_ID, record(1L, "created"))))
                .isNotEqualTo(
                        defaultHash.hashcode(
                                DataChangeEvent.insertEvent(TABLE_ID, record(1L, "created"))));
    }

    @Test
    void testUpdateWithPrimaryKeyChangeHashesByAfterKey() {
        HashFunction<DataChangeEvent> hashFunction =
                createHashFunction(MilvusTestUtils.defaultConfig());

        int changedKeyUpdateHash =
                hashFunction.hashcode(
                        DataChangeEvent.updateEvent(
                                TABLE_ID, record(1L, "old"), record(2L, "new")));
        int afterKeyInsertHash =
                hashFunction.hashcode(DataChangeEvent.insertEvent(TABLE_ID, record(2L, "new")));

        Assertions.assertThat(changedKeyUpdateHash).isEqualTo(afterKeyInsertHash);
    }

    @Test
    void testAllowPrimaryKeyChangeHashesByCollectionOnly() {
        HashFunction<DataChangeEvent> hashFunction =
                createHashFunction(allowPrimaryKeyChangeConfig());

        int firstHash =
                hashFunction.hashcode(DataChangeEvent.insertEvent(TABLE_ID, record(1L, "created")));
        int secondHash =
                hashFunction.hashcode(DataChangeEvent.insertEvent(TABLE_ID, record(2L, "created")));
        int changedKeyUpdateHash =
                hashFunction.hashcode(
                        DataChangeEvent.updateEvent(
                                TABLE_ID, record(1L, "old"), record(2L, "new")));

        Assertions.assertThat(secondHash).isEqualTo(firstHash);
        Assertions.assertThat(changedKeyUpdateHash).isEqualTo(firstHash);
    }

    private static HashFunction<DataChangeEvent> createHashFunction(MilvusDataSinkConfig config) {
        return new MilvusHashFunctionProvider(config).getHashFunction(TABLE_ID, SCHEMA);
    }

    private static GenericRecordData record(long id, String title) {
        return GenericRecordData.of(
                id,
                BinaryStringData.fromString(title),
                new GenericArrayData(new float[] {1.0f, 2.0f, 3.0f}));
    }

    private static MilvusDataSinkConfig allowPrimaryKeyChangeConfig() {
        return new MilvusDataSinkConfig(
                "http://localhost:19530",
                "",
                Duration.ofSeconds(10),
                Duration.ofSeconds(60),
                "default",
                Collections.emptyMap(),
                true,
                true,
                true,
                false,
                false,
                "",
                false,
                1024,
                Collections.emptyList(),
                MilvusTestUtils.defaultConfig().getVectorFields(),
                Collections.emptyMap(),
                "",
                65535,
                100,
                Duration.ofSeconds(10),
                0,
                Duration.ZERO,
                true,
                10,
                true,
                "allow",
                false,
                "",
                "AUTOINDEX",
                "COSINE",
                Collections.emptyMap());
    }
}
