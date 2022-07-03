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

package com.ververica.cdc.connectors.base.source.reader.external;

import org.apache.flink.table.types.logical.RowType;

import com.ververica.cdc.connectors.base.config.JdbcSourceConfig;
import com.ververica.cdc.connectors.base.config.SourceConfig;
import com.ververica.cdc.connectors.base.dialect.JdbcDataSourceDialect;
import com.ververica.cdc.connectors.base.relational.JdbcSourceEventDispatcher;
import com.ververica.cdc.connectors.base.source.meta.offset.Offset;
import com.ververica.cdc.connectors.base.source.meta.split.SourceSplitBase;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.RelationalDatabaseSchema;
import io.debezium.relational.RelationalTableFilters;
import io.debezium.relational.Table;
import io.debezium.util.SchemaNameAdjuster;
import org.apache.kafka.connect.source.SourceRecord;

/** The context for fetch task that fetching data of snapshot split from JDBC data source. */
public abstract class JdbcSourceFetchTaskContext implements FetchTask.Context {

    protected final JdbcSourceConfig sourceConfig;
    protected final JdbcDataSourceDialect dataSourceDialect;
    protected final CommonConnectorConfig dbzConnectorConfig;
    protected final SchemaNameAdjuster schemaNameAdjuster;

    public JdbcSourceFetchTaskContext(
            JdbcSourceConfig sourceConfig, JdbcDataSourceDialect dataSourceDialect) {
        this.sourceConfig = sourceConfig;
        this.dataSourceDialect = dataSourceDialect;
        this.dbzConnectorConfig = sourceConfig.getDbzConnectorConfig();
        this.schemaNameAdjuster = SchemaNameAdjuster.create();
    }

    public abstract void configure(SourceSplitBase sourceSplitBase);

    public SourceConfig getSourceConfig() {
        return sourceConfig;
    }

    public JdbcDataSourceDialect getDataSourceDialect() {
        return dataSourceDialect;
    }

    public CommonConnectorConfig getDbzConnectorConfig() {
        return dbzConnectorConfig;
    }

    public SchemaNameAdjuster getSchemaNameAdjuster() {
        return null;
    }

    public abstract RelationalDatabaseSchema getDatabaseSchema();

    public abstract RowType getSplitType(Table table);

    public abstract ErrorHandler getErrorHandler();

    public abstract JdbcSourceEventDispatcher getDispatcher();

    public abstract OffsetContext getOffsetContext();

    public abstract ChangeEventQueue<DataChangeEvent> getQueue();

    public abstract Offset getStreamOffset(SourceRecord sourceRecord);

    public abstract RelationalTableFilters getRelationalTableFilters();
}
