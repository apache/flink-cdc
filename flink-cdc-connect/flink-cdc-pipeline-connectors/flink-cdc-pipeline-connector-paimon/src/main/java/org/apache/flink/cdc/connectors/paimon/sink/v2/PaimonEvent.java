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

package org.apache.flink.cdc.connectors.paimon.sink.v2;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.GenericRow;

/** Contains the data to be written for {@link PaimonWriter}. */
public class PaimonEvent {

    // Identifier for the Paimon table to be written.
    Identifier tableId;

    // The actual record to be written to Paimon table.
    GenericRow genericRow;

    // if true, means that table schema has changed right before this genericRow.
    boolean shouldRefreshSchema;
    int bucket;

    public PaimonEvent(Identifier tableId, GenericRow genericRow) {
        this.tableId = tableId;
        this.genericRow = genericRow;
        this.shouldRefreshSchema = false;
    }

    public PaimonEvent(Identifier tableId, GenericRow genericRow, boolean shouldRefreshSchema) {
        this.tableId = tableId;
        this.genericRow = genericRow;
        this.shouldRefreshSchema = shouldRefreshSchema;
    }

    public PaimonEvent(
            Identifier tableId, GenericRow genericRow, boolean shouldRefreshSchema, int bucket) {
        this.tableId = tableId;
        this.genericRow = genericRow;
        this.shouldRefreshSchema = shouldRefreshSchema;
        this.bucket = bucket;
    }

    public Identifier getTableId() {
        return tableId;
    }

    public void setTableId(Identifier tableId) {
        this.tableId = tableId;
    }

    public boolean isShouldRefreshSchema() {
        return shouldRefreshSchema;
    }

    public void setShouldRefreshSchema(boolean shouldRefreshSchema) {
        this.shouldRefreshSchema = shouldRefreshSchema;
    }

    public GenericRow getGenericRow() {
        return genericRow;
    }

    public void setGenericRow(GenericRow genericRow) {
        this.genericRow = genericRow;
    }

    public int getBucket() {
        return bucket;
    }

    public void setBucket(int bucket) {
        this.bucket = bucket;
    }
}
