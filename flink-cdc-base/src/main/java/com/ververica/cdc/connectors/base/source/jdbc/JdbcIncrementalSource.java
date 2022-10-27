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

package com.ververica.cdc.connectors.base.source.jdbc;

import com.ververica.cdc.connectors.base.config.JdbcSourceConfig;
import com.ververica.cdc.connectors.base.config.JdbcSourceConfigFactory;
import com.ververica.cdc.connectors.base.dialect.JdbcDataSourceDialect;
import com.ververica.cdc.connectors.base.source.IncrementalSource;
import com.ververica.cdc.connectors.base.source.meta.offset.OffsetFactory;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;

/**
 * The basic source of Incremental Snapshot framework for JDBC datasource, it is based on FLIP-27
 * and Watermark Signal Algorithm which supports parallel reading snapshot of table and then
 * continue to capture data change by streaming reading.
 */
public class JdbcIncrementalSource<T> extends IncrementalSource<T, JdbcSourceConfig> {

    public JdbcIncrementalSource(
            JdbcSourceConfigFactory configFactory,
            DebeziumDeserializationSchema<T> deserializationSchema,
            OffsetFactory offsetFactory,
            JdbcDataSourceDialect dataSourceDialect) {
        super(configFactory, deserializationSchema, offsetFactory, dataSourceDialect);
    }
}
