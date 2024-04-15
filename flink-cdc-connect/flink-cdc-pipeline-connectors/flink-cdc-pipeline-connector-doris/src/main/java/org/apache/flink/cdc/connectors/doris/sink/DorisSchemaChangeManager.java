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
import org.apache.doris.flink.sink.schema.SchemaChangeManager;

import java.io.IOException;

/** Provides schema change operations based on {@link SchemaChangeManager}. */
public class DorisSchemaChangeManager extends SchemaChangeManager {
    public DorisSchemaChangeManager(DorisOptions dorisOptions) {
        super(dorisOptions);
    }

    private static final String MODIFY_COLUMN_DDL = "ALTER TABLE %s MODIFY COLUMN %s %s";

    // Error message response from Doris server when no alter change is applied.
    private static final String ALTER_NO_CHANGE_ERROR_MESSAGE =
            "Nothing is changed. please check your alter stmt.";

    public boolean alterColumn(
            String database, String table, String columnName, String newColumnType)
            throws IOException, IllegalArgumentException {
        String tableIdentifier = String.format("%s.%s", database, table);
        FieldSchema alterFieldSchema = new FieldSchema(columnName, newColumnType, "");

        String alterColumnDDL =
                String.format(
                        MODIFY_COLUMN_DDL,
                        tableIdentifier,
                        columnName,
                        alterFieldSchema.getTypeString());

        try {
            return this.schemaChange(
                    database, table, buildRequestParam(true, columnName), alterColumnDDL);
        } catch (DorisSchemaChangeException ex) {
            if (ex.getMessage().contains(ALTER_NO_CHANGE_ERROR_MESSAGE)) {
                // Doris doesn't allow ALTER statement without effects.
                // We should ignore such exception since upstream connector
                // might emit redundant AlterColumnTypeEvents.
                return false;
            } else {
                throw ex;
            }
        }
    }
}
