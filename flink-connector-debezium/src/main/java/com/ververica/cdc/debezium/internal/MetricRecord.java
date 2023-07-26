/*
 * Copyright 2023 Ververica Inc.
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

import org.apache.kafka.connect.source.SourceRecord;

/** Records used for carrying metric information. */
public class MetricRecord extends SourceRecord {

    private final String metricKey;

    private final Object metricValue;

    public MetricRecord(String metricKey, Object metricValue) {
        super(null, null, null, null, null);
        this.metricKey = metricKey;
        this.metricValue = metricValue;
    }

    public String getMetricKey() {
        return metricKey;
    }

    public Object getMetricValue() {
        return metricValue;
    }
}
