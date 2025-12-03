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

package org.apache.flink.cdc.connectors.paimon.sink;

import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.function.HashFunction;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.connectors.paimon.sink.v2.PaimonWriterHelper;

import org.apache.paimon.AppendOnlyFileStore;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.flink.FlinkCatalogFactory;
import org.apache.paimon.flink.sink.RowAssignerChannelComputer;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;

import java.io.Serializable;
import java.time.ZoneId;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * A {@link HashFunction} implementation for {@link PaimonDataSink}. Shuffle {@link DataChangeEvent}
 * by hash of PrimaryKey.
 */
public class PaimonHashFunction implements HashFunction<DataChangeEvent>, Serializable {

    private static final long serialVersionUID = 1L;

    private final List<RecordData.FieldGetter> fieldGetters;

    private final RowAssignerChannelComputer channelComputer;

    private final int parallelism;

    public PaimonHashFunction(
            Options options, TableId tableId, Schema schema, ZoneId zoneId, int parallelism) {
        this.parallelism = parallelism;
        Catalog catalog = FlinkCatalogFactory.createPaimonCatalog(options);
        FileStoreTable table;
        try {
            table = (FileStoreTable) catalog.getTable(Identifier.fromString(tableId.toString()));
        } catch (Catalog.TableNotExistException e) {
            throw new RuntimeException(e);
        }
        if (table instanceof AppendOnlyFileStore) {
            this.fieldGetters = null;
            channelComputer = null;
        } else {
            this.fieldGetters = PaimonWriterHelper.createFieldGetters(schema, zoneId);
            channelComputer = new RowAssignerChannelComputer(table.schema(), parallelism);
            channelComputer.setup(parallelism);
        }
    }

    @Override
    public int hashcode(DataChangeEvent event) {
        if (channelComputer != null) {
            GenericRow genericRow =
                    PaimonWriterHelper.convertEventToGenericRow(event, fieldGetters);
            return channelComputer.channel(genericRow);
        } else {
            // Avoid sending all events to the same subtask when table has no primary key.
            return ThreadLocalRandom.current().nextInt(parallelism);
        }
    }
}
