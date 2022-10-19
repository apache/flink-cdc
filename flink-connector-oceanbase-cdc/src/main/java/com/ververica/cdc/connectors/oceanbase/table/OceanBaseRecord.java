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

package com.ververica.cdc.connectors.oceanbase.table;

import com.oceanbase.oms.logmessage.ByteString;
import com.oceanbase.oms.logmessage.DataMessage;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** An internal data structure representing record of OceanBase. */
public class OceanBaseRecord implements Serializable {
    private static final long serialVersionUID = 1L;

    private final SourceInfo sourceInfo;
    private final boolean isSnapshotRecord;
    private Map<String, Object> jdbcFields;
    private DataMessage.Record.Type opt;
    private Map<String, ByteString> logMessageFieldsBefore;
    private Map<String, ByteString> logMessageFieldsAfter;

    public OceanBaseRecord(SourceInfo sourceInfo, Map<String, Object> jdbcFields) {
        this.sourceInfo = sourceInfo;
        this.isSnapshotRecord = true;
        this.jdbcFields = jdbcFields;
    }

    public OceanBaseRecord(
            SourceInfo sourceInfo,
            DataMessage.Record.Type opt,
            List<DataMessage.Record.Field> logMessageFieldList) {
        this.sourceInfo = sourceInfo;
        this.isSnapshotRecord = false;
        this.opt = opt;
        this.logMessageFieldsBefore = new HashMap<>();
        this.logMessageFieldsAfter = new HashMap<>();
        for (DataMessage.Record.Field field : logMessageFieldList) {
            if (field.isPrev()) {
                logMessageFieldsBefore.put(field.getFieldname(), field.getValue());
            } else {
                logMessageFieldsAfter.put(field.getFieldname(), field.getValue());
            }
        }
    }

    public SourceInfo getSourceInfo() {
        return sourceInfo;
    }

    public boolean isSnapshotRecord() {
        return isSnapshotRecord;
    }

    public Map<String, Object> getJdbcFields() {
        return jdbcFields;
    }

    public DataMessage.Record.Type getOpt() {
        return opt;
    }

    public Map<String, ByteString> getLogMessageFieldsBefore() {
        return logMessageFieldsBefore;
    }

    public Map<String, ByteString> getLogMessageFieldsAfter() {
        return logMessageFieldsAfter;
    }

    /** Information about the source of record. */
    public static class SourceInfo implements Serializable {
        private static final long serialVersionUID = 1L;

        private final String tenant;
        private final String database;
        private final String table;
        private final long timestampS;

        public SourceInfo(String tenant, String database, String table, long timestampS) {
            this.tenant = tenant;
            this.database = database;
            this.table = table;
            this.timestampS = timestampS;
        }

        public String getTenant() {
            return tenant;
        }

        public String getDatabase() {
            return database;
        }

        public String getTable() {
            return table;
        }

        public long getTimestampS() {
            return timestampS;
        }
    }
}
