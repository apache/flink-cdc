/*
 * Copyright 2022 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.debezium.internal;

import io.debezium.document.Document;
import io.debezium.document.DocumentWriter;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.relational.history.TableChanges.TableChange;

import javax.annotation.Nullable;

import java.io.IOException;

/**
 * The Record represents a schema change event, it contains either one {@link HistoryRecord} or
 * {@link TableChange}.
 *
 * <p>The {@link HistoryRecord} will be used by {@link FlinkDatabaseHistory} which keeps full
 * history of table change events for all tables, the {@link TableChange} will be used by {@link
 * FlinkDatabaseSchemaHistory} which keeps the latest table change for each table.
 */
public class SchemaRecord {

    @Nullable private final HistoryRecord historyRecord;

    @Nullable private final Document tableChangeDoc;

    public SchemaRecord(HistoryRecord historyRecord) {
        this.historyRecord = historyRecord;
        this.tableChangeDoc = null;
    }

    public SchemaRecord(Document document) {
        if (isHistoryRecordDocument(document)) {
            this.historyRecord = new HistoryRecord(document);
            this.tableChangeDoc = null;
        } else {
            this.tableChangeDoc = document;
            this.historyRecord = null;
        }
    }

    @Nullable
    public HistoryRecord getHistoryRecord() {
        return historyRecord;
    }

    @Nullable
    public Document getTableChangeDoc() {
        return tableChangeDoc;
    }

    public boolean isHistoryRecord() {
        return historyRecord != null;
    }

    public boolean isTableChangeRecord() {
        return tableChangeDoc != null;
    }

    public Document toDocument() {
        if (historyRecord != null) {
            return historyRecord.document();
        } else {
            return tableChangeDoc;
        }
    }

    @Override
    public String toString() {
        try {
            return DocumentWriter.defaultWriter().write(toDocument());
        } catch (IOException e) {
            return super.toString();
        }
    }

    private boolean isHistoryRecordDocument(Document document) {
        return new HistoryRecord(document).isValid();
    }
}
