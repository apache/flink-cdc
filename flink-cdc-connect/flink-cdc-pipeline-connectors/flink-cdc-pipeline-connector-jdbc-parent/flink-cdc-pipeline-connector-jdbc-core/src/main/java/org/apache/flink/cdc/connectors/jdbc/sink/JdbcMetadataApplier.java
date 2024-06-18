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

package org.apache.flink.cdc.connectors.jdbc.sink;

import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.DropTableEvent;
import org.apache.flink.cdc.common.event.RenameColumnEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TruncateTableEvent;
import org.apache.flink.cdc.common.exceptions.SchemaEvolveException;
import org.apache.flink.cdc.common.exceptions.UnsupportedSchemaChangeEventException;
import org.apache.flink.cdc.common.sink.MetadataApplier;
import org.apache.flink.cdc.connectors.jdbc.dialect.JdbcSinkDialect;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Apply schema change events to JDBC-like sinks. */
public class JdbcMetadataApplier implements MetadataApplier {
    private static final Logger LOG = LoggerFactory.getLogger(JdbcMetadataApplier.class);

    private final JdbcSinkDialect dialect;

    public JdbcMetadataApplier(JdbcSinkDialect dialect) {
        this.dialect = dialect;
    }

    @Override
    public void applySchemaChange(SchemaChangeEvent event) {
        LOG.info("Applying schema change event: {}", event);
        try {
            if (event instanceof CreateTableEvent) {
                applyCreateTableEvent((CreateTableEvent) event);
            } else if (event instanceof AddColumnEvent) {
                applyAddColumnEvent((AddColumnEvent) event);
            } else if (event instanceof DropColumnEvent) {
                applyDropColumnEvent((DropColumnEvent) event);
            } else if (event instanceof RenameColumnEvent) {
                applyRenameColumnEvent((RenameColumnEvent) event);
            } else if (event instanceof AlterColumnTypeEvent) {
                applyAlterColumnType((AlterColumnTypeEvent) event);
            } else if (event instanceof TruncateTableEvent) {
                applyTruncateTableEvent((TruncateTableEvent) event);
            } else if (event instanceof DropTableEvent) {
                applyDropTableEvent((DropTableEvent) event);
            } else {
                throw new UnsupportedSchemaChangeEventException(event);
            }
        } catch (Exception ex) {
            throw new SchemaEvolveException(event, "Failed to apply schema change event. ", ex);
        }
    }

    private void applyCreateTableEvent(CreateTableEvent event) {
        dialect.createTable(event, true);
    }

    private void applyAddColumnEvent(AddColumnEvent event) {
        dialect.addColumn(event);
    }

    private void applyAlterColumnType(AlterColumnTypeEvent event) {
        dialect.alterColumnType(event);
    }

    private void applyRenameColumnEvent(RenameColumnEvent event) {
        dialect.renameColumn(event);
    }

    private void applyDropColumnEvent(DropColumnEvent event) {
        dialect.dropColumn(event);
    }

    private void applyTruncateTableEvent(TruncateTableEvent event) {
        dialect.truncateTable(event);
    }

    private void applyDropTableEvent(DropTableEvent event) {
        dialect.dropTable(event, true);
    }
}
