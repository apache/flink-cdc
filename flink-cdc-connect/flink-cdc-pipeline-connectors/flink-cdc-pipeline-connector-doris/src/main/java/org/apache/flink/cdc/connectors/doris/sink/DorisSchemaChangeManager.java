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

package org.apache.flink.cdc.connectors.doris.sink;

import org.apache.doris.flink.catalog.doris.FieldSchema;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.exception.DorisSchemaChangeException;
import org.apache.doris.flink.exception.IllegalArgumentException;
import org.apache.doris.flink.sink.schema.AddColumnPosition;
import org.apache.doris.flink.sink.schema.SchemaChangeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Locale;

import static org.apache.doris.flink.catalog.doris.DorisSchemaFactory.identifier;

/** An enriched version of Doris' {@link SchemaChangeManager}. */
public class DorisSchemaChangeManager extends SchemaChangeManager {
    private static final Logger LOG = LoggerFactory.getLogger(DorisSchemaChangeManager.class);

    public DorisSchemaChangeManager(DorisOptions dorisOptions, String charsetEncoding) {
        super(dorisOptions, charsetEncoding);
    }

    @Override
    public boolean addColumn(
            String databaseName,
            String tableName,
            FieldSchema addFieldSchema,
            AddColumnPosition position)
            throws IOException, IllegalArgumentException {
        try {
            return super.addColumn(databaseName, tableName, addFieldSchema, position);
        } catch (DorisSchemaChangeException e) {
            if (isLastPosition(position) || !isColumnPositionException(e)) {
                throw e;
            }
            LOG.warn(
                    "Failed to apply ADD COLUMN with Doris column position. "
                            + "Fallback to ADD COLUMN without position. database={}, table={}, column={}, "
                            + "positionType={}, referenceColumn={}, cause={}",
                    databaseName,
                    tableName,
                    addFieldSchema.getName(),
                    position.getPositionType(),
                    position.getReferenceColumn(),
                    e.getMessage());
            return super.addColumn(
                    databaseName, tableName, addFieldSchema, AddColumnPosition.last());
        }
    }

    public boolean truncateTable(String databaseName, String tableName)
            throws IOException, IllegalArgumentException {
        String truncateTableDDL =
                "TRUNCATE TABLE " + identifier(databaseName) + "." + identifier(tableName);
        return this.execute(truncateTableDDL, databaseName);
    }

    public boolean dropTable(String databaseName, String tableName)
            throws IOException, IllegalArgumentException {
        String dropTableDDL =
                "DROP TABLE " + identifier(databaseName) + "." + identifier(tableName);
        return this.execute(dropTableDDL, databaseName);
    }

    public boolean alterTableComment(String databaseName, String tableName, String comment)
            throws IOException, IllegalArgumentException {
        String alterTableCommentDDL =
                "ALTER TABLE "
                        + identifier(databaseName)
                        + "."
                        + identifier(tableName)
                        + " MODIFY COMMENT "
                        + quoted(comment);
        return this.execute(alterTableCommentDDL, databaseName);
    }

    private String quoted(String str) {
        String escaped = str.replace("\\", "\\\\");
        return "\"" + escaped.replace("\"", "\\\"") + "\"";
    }

    private boolean isLastPosition(AddColumnPosition position) {
        return position == null
                || AddColumnPosition.PositionType.LAST.equals(position.getPositionType());
    }

    private boolean isColumnPositionException(Throwable throwable) {
        StringBuilder message = new StringBuilder();
        Throwable current = throwable;
        while (current != null) {
            if (current.getMessage() != null) {
                message.append(current.getMessage()).append(' ');
            }
            current = current.getCause();
        }
        String lowerMessage = message.toString().toLowerCase(Locale.ROOT);
        return lowerMessage.contains("as first column")
                || lowerMessage.contains("before key column")
                || lowerMessage.contains("after value column")
                || lowerMessage.contains("modify column position after itself")
                || lowerMessage.contains("modify position is invalid");
    }
}
