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

package org.apache.flink.cdc.connectors.tdengine.serde;

import org.apache.flink.cdc.connectors.tdengine.utils.TDengineTableInfo;

import java.util.List;

/** Converted TDengine row ready for SQL rendering. */
public class TDengineRowData {

    private final TDengineTableInfo tableInfo;
    private final String subtableName;
    private final Object timestampValue;
    private final List<String> tagColumnNames;
    private final List<Object> tagValues;
    private final List<String> metricColumnNames;
    private final List<Object> metricValues;

    public TDengineRowData(
            TDengineTableInfo tableInfo,
            String subtableName,
            Object timestampValue,
            List<String> tagColumnNames,
            List<Object> tagValues,
            List<String> metricColumnNames,
            List<Object> metricValues) {
        this.tableInfo = tableInfo;
        this.subtableName = subtableName;
        this.timestampValue = timestampValue;
        this.tagColumnNames = tagColumnNames;
        this.tagValues = tagValues;
        this.metricColumnNames = metricColumnNames;
        this.metricValues = metricValues;
    }

    public TDengineTableInfo getTableInfo() {
        return tableInfo;
    }

    public String getSubtableName() {
        return subtableName;
    }

    public Object getTimestampValue() {
        return timestampValue;
    }

    public List<String> getTagColumnNames() {
        return tagColumnNames;
    }

    public List<Object> getTagValues() {
        return tagValues;
    }

    public List<String> getMetricColumnNames() {
        return metricColumnNames;
    }

    public List<Object> getMetricValues() {
        return metricValues;
    }
}
