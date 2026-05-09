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

package org.apache.flink.cdc.connectors.paimon.sink.testsource;

import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** Source function for testing append-only tables. */
public class AppendOnlyTableSourceFunction extends RichParallelSourceFunction<Event> {

    private List<Event> testEventList;

    public static final TableId TEST_TABLE_ID = TableId.tableId("default_database", "table1");

    private static final List<Column> SIMPLE_COLUMNS =
            Arrays.asList(
                    Column.physicalColumn("uuid", DataTypes.STRING().notNull()),
                    Column.physicalColumn("path", DataTypes.STRING()),
                    Column.physicalColumn("blobContent", DataTypes.VARBINARY(1000)));

    // Fixed UUIDs for testing
    private static final String UUID_1 = "550e8400-e29b-41d4-a716-446655440000";
    private static final String UUID_2 = "6ba7b810-9dad-11d1-80b4-00c04fd430c8";
    private static final String UUID_3 = "f47ac10b-58cc-4372-a567-0e02b2c3d479";

    // OSS file paths for testing
    private static final String PATH_1 = "oss://my-bucket/data/files/document_001.pdf";
    private static final String PATH_2 = "oss://my-bucket/data/images/photo_002.jpg";
    private static final String PATH_3 = "oss://my-bucket/data/videos/movie_003.mp4";

    // Blob content for testing
    private static final byte[] BLOB_CONTENT_1 = "PDF binary content for document 001".getBytes();
    private static final byte[] BLOB_CONTENT_2 = "JPG binary content for photo 002".getBytes();
    private static final byte[] BLOB_CONTENT_3 = "MP4 binary content for movie 003".getBytes();

    public AppendOnlyTableSourceFunction() {}

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        testEventList = new ArrayList<>();

        Schema schema = Schema.newBuilder().setColumns(SIMPLE_COLUMNS).build();
        testEventList.add(new CreateTableEvent(TEST_TABLE_ID, schema));

        BinaryRecordDataGenerator binaryRecordDataGenerator =
                new BinaryRecordDataGenerator(
                        schema.getColumnDataTypes()
                                .toArray(new org.apache.flink.cdc.common.types.DataType[0]));

        // Emit 3 insert events with uuid, path and blobContent
        testEventList.add(
                DataChangeEvent.insertEvent(
                        TEST_TABLE_ID,
                        binaryRecordDataGenerator.generate(
                                new Object[] {
                                    BinaryStringData.fromString(UUID_1),
                                    BinaryStringData.fromString(PATH_1),
                                    BLOB_CONTENT_1
                                })));
        testEventList.add(
                DataChangeEvent.insertEvent(
                        TEST_TABLE_ID,
                        binaryRecordDataGenerator.generate(
                                new Object[] {
                                    BinaryStringData.fromString(UUID_2),
                                    BinaryStringData.fromString(PATH_2),
                                    BLOB_CONTENT_2
                                })));
        testEventList.add(
                DataChangeEvent.insertEvent(
                        TEST_TABLE_ID,
                        binaryRecordDataGenerator.generate(
                                new Object[] {
                                    BinaryStringData.fromString(UUID_3),
                                    BinaryStringData.fromString(PATH_3),
                                    BLOB_CONTENT_3
                                })));
    }

    @Override
    public void run(
            org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext<Event>
                    context) {
        // Emit all events in the first subtask
        if (getRuntimeContext().getTaskInfo().getIndexOfThisSubtask() == 0) {
            for (Event event : testEventList) {
                context.collect(event);
            }
        }
    }

    @Override
    public void cancel() {
        // Do nothing
    }
}
