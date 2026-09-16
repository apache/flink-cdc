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

package com.github.shyiko.mysql.binlog.event.deserialization;

import com.github.shyiko.mysql.binlog.event.DeleteRowsEventData;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream;

import java.io.IOException;
import java.io.Serializable;
import java.util.BitSet;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Copied from mysql-binlog-connector 0.27.2 to add a {@link TableIdFilter}.
 *
 * <p>Line 52-56: Add a new constructor with {@link TableIdFilter} supplied.
 *
 * <p>Line 70-74: Use a {@link TableIdFilter} to skip the binlog deserialization of unwanted tables.
 */
public class DeleteRowsEventDataDeserializer
        extends AbstractRowsEventDataDeserializer<DeleteRowsEventData> {

    private boolean mayContainExtraInformation;

    /** the table id filter to skip further deserialization of unsubscribed table ids. */
    private final TableIdFilter tableIdFilter;

    public DeleteRowsEventDataDeserializer(Map<Long, TableMapEventData> tableMapEventByTableId) {
        this(tableMapEventByTableId, TableIdFilter.all());
    }

    public DeleteRowsEventDataDeserializer(
            Map<Long, TableMapEventData> tableMapEventByTableId, TableIdFilter tableIdFilter) {
        super(tableMapEventByTableId);
        this.tableIdFilter = tableIdFilter;
    }

    public DeleteRowsEventDataDeserializer setMayContainExtraInformation(
            boolean mayContainExtraInformation) {
        this.mayContainExtraInformation = mayContainExtraInformation;
        return this;
    }

    @Override
    public DeleteRowsEventData deserialize(ByteArrayInputStream inputStream) throws IOException {
        DeleteRowsEventData eventData = new DeleteRowsEventData();
        eventData.setTableId(inputStream.readLong(6));

        // skip further deserialization if the table id is unsubscribed
        if (!tableIdFilter.test(eventData.getTableId())) {
            eventData.setIncludedColumns(null);
            eventData.setRows(Collections.emptyList());
            return eventData;
        }

        inputStream.readInteger(2); // reserved
        if (mayContainExtraInformation) {
            int extraInfoLength = inputStream.readInteger(2);
            inputStream.skip(extraInfoLength - 2);
        }
        int numberOfColumns = inputStream.readPackedInteger();
        eventData.setIncludedColumns(inputStream.readBitSet(numberOfColumns, true));
        eventData.setRows(
                deserializeRows(
                        eventData.getTableId(), eventData.getIncludedColumns(), inputStream));
        return eventData;
    }

    private List<Serializable[]> deserializeRows(
            long tableId, BitSet includedColumns, ByteArrayInputStream inputStream)
            throws IOException {
        List<Serializable[]> result = new LinkedList<Serializable[]>();
        while (inputStream.available() > 0) {
            result.add(deserializeRow(tableId, includedColumns, inputStream));
        }
        return result;
    }
}
