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

package org.apache.flink.cdc.connectors.tdengine.sink;

import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.AlterTableCommentEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEventType;
import org.apache.flink.cdc.common.event.visitor.SchemaChangeEventVisitor;
import org.apache.flink.cdc.common.exceptions.SchemaEvolveException;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.sink.MetadataApplier;
import org.apache.flink.cdc.connectors.tdengine.utils.TDengineSchemaValidator;
import org.apache.flink.cdc.connectors.tdengine.utils.TDengineSqlUtils;
import org.apache.flink.cdc.connectors.tdengine.utils.TDengineTableInfo;

import org.apache.flink.shaded.guava31.com.google.common.collect.Sets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.Set;

import static org.apache.flink.cdc.common.event.SchemaChangeEventType.ADD_COLUMN;
import static org.apache.flink.cdc.common.event.SchemaChangeEventType.ALTER_TABLE_COMMENT;
import static org.apache.flink.cdc.common.event.SchemaChangeEventType.CREATE_TABLE;

/** Applies schema changes to TDengine databases and super tables. */
public class TDengineMetadataApplier implements MetadataApplier {

    private static final Logger LOG = LoggerFactory.getLogger(TDengineMetadataApplier.class);

    private final TDengineDataSinkConfig config;
    private final TDengineClientFactory clientFactory;
    private Set<SchemaChangeEventType> enabledSchemaEvolutionTypes;

    public TDengineMetadataApplier(TDengineDataSinkConfig config) {
        this(config, DefaultTDengineClientWrapper::new);
    }

    TDengineMetadataApplier(TDengineDataSinkConfig config, TDengineClientFactory clientFactory) {
        this.config = config;
        this.clientFactory = clientFactory;
        this.enabledSchemaEvolutionTypes = getSupportedSchemaEvolutionTypes();
    }

    @Override
    public MetadataApplier setAcceptedSchemaEvolutionTypes(
            Set<SchemaChangeEventType> schemaEvolutionTypes) {
        this.enabledSchemaEvolutionTypes = schemaEvolutionTypes;
        return this;
    }

    @Override
    public boolean acceptsSchemaEvolutionType(SchemaChangeEventType schemaChangeEventType) {
        return enabledSchemaEvolutionTypes.contains(schemaChangeEventType);
    }

    @Override
    public Set<SchemaChangeEventType> getSupportedSchemaEvolutionTypes() {
        return Sets.newHashSet(CREATE_TABLE, ADD_COLUMN, ALTER_TABLE_COMMENT);
    }

    @Override
    public void applySchemaChange(SchemaChangeEvent schemaChangeEvent)
            throws SchemaEvolveException {
        if (!acceptsSchemaEvolutionType(schemaChangeEvent.getType())) {
            return;
        }
        try {
            SchemaChangeEventVisitor.<Void, Exception>visit(
                    schemaChangeEvent,
                    addColumnEvent -> {
                        applyAddColumn(addColumnEvent);
                        return null;
                    },
                    ignored -> {
                        throw new UnsupportedOperationException(
                                "ALTER_COLUMN_TYPE is not supported by TDengine sink.");
                    },
                    createTableEvent -> {
                        applyCreateTable(createTableEvent);
                        return null;
                    },
                    ignored -> {
                        throw new UnsupportedOperationException(
                                "DROP_COLUMN is not supported by TDengine sink.");
                    },
                    ignored -> {
                        throw new UnsupportedOperationException(
                                "DROP_TABLE is not supported by TDengine sink.");
                    },
                    ignored -> {
                        throw new UnsupportedOperationException(
                                "RENAME_COLUMN is not supported by TDengine sink.");
                    },
                    ignored -> {
                        throw new UnsupportedOperationException(
                                "TRUNCATE_TABLE is not supported by TDengine sink.");
                    },
                    alterTableCommentEvent -> {
                        applyAlterTableComment(alterTableCommentEvent);
                        return null;
                    });
        } catch (Exception e) {
            throw new SchemaEvolveException(schemaChangeEvent, e.getMessage(), e);
        }
    }

    private void applyCreateTable(CreateTableEvent event) throws SQLException {
        TDengineTableInfo tableInfo = TDengineSqlUtils.resolveTableInfo(event.tableId(), config);
        try (TDengineClientWrapper client = clientFactory.create(config)) {
            if (config.isCreateDatabaseEnabled()) {
                client.execute(TDengineSqlUtils.createDatabaseSql(config));
            }
            if (config.isCreateStableEnabled()) {
                client.execute(
                        TDengineSqlUtils.createStableSql(tableInfo, event.getSchema(), config));
            }
            if (config.isStableSchemaValidationEnabled()) {
                TDengineSchemaValidator.validateStableSchema(
                        client, tableInfo, event.getSchema(), config);
            }
        }
    }

    private void applyAddColumn(AddColumnEvent event) throws SQLException {
        TDengineTableInfo tableInfo = TDengineSqlUtils.resolveTableInfo(event.tableId(), config);
        try (TDengineClientWrapper client = clientFactory.create(config)) {
            for (AddColumnEvent.ColumnWithPosition columnWithPosition : event.getAddedColumns()) {
                Column column = columnWithPosition.getAddColumn();
                if (column.getName().equals(config.getTimestampField())
                        || column.getName().equals(config.getSubtableField())
                        || config.getTagFields().contains(column.getName())) {
                    throw new UnsupportedOperationException(
                            "TDengine sink does not support adding timestamp, subtable or tag columns.");
                }
                client.execute(TDengineSqlUtils.addColumnSql(tableInfo, column, config));
            }
        }
    }

    private void applyAlterTableComment(AlterTableCommentEvent event) {
        LOG.debug("Ignoring table comment change for TDengine sink: {}", event);
    }
}
