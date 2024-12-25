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

package org.apache.flink.cdc.connectors.mongodb.utils;

import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.types.RowType;
import org.apache.flink.cdc.connectors.mongodb.source.config.MongoDBSourceConfig;
import org.apache.flink.cdc.connectors.mongodb.source.utils.MongoUtils;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import io.debezium.relational.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Utilities for converting from debezium {@link Table} types to {@link Schema}. */
public class MongoDBSchemaUtils {

    private static final Logger LOG = LoggerFactory.getLogger(MongoDBSchemaUtils.class);

    public static List<String> listDatabases(MongoDBSourceConfig sourceConfig) {
        try (MongoClient client = MongoUtils.clientFor(sourceConfig)) {
            return listDatabases(client);
        } catch (SQLException e) {
            throw new RuntimeException("Error to list databases: " + e.getMessage(), e);
        }
    }

    public static List<TableId> listTables(
            MongoDBSourceConfig sourceConfig, @Nullable String dbName) {
        try (MongoClient client = MongoUtils.clientFor(sourceConfig)) {
            final List<MongoDatabase> databaseList = new ArrayList<>();
            if (dbName != null) {
                databaseList.add(client.getDatabase(dbName));
            } else {
                listDatabases(client).forEach(db -> databaseList.add(client.getDatabase(db)));
            }
            List<TableId> tableIds = new ArrayList<>();
            for (MongoDatabase db : databaseList) {
                tableIds.addAll(listTables(db));
            }
            return tableIds;
        } catch (SQLException e) {
            throw new RuntimeException("Error to list databases: " + e.getMessage(), e);
        }
    }

    public static Schema getTableSchema(MongoDBSourceConfig sourceConfig, TableId tableId) {
        return MongoDBSchemaUtils.getJsonSchema();
    }

    public static List<String> listDatabases(MongoClient client) throws SQLException {
        // -------------------
        // READ DATABASE NAMES
        // -------------------
        // Get the list of databases ...
        LOG.info("Read list of available databases");
        final List<String> databaseNames = new ArrayList<>();
        client.listDatabaseNames().forEach(databaseNames::add);
        LOG.info("\t list of available databases are: {}", databaseNames);
        return databaseNames;
    }

    public static List<TableId> listTables(MongoDatabase db) throws SQLException {
        // ----------------
        // READ TABLE NAMES
        // ----------------
        // Get the list of table IDs for each database.
        LOG.info("Read list of available tables in {}", db.getName());
        final List<TableId> tableIds = new ArrayList<>();
        db.listCollectionNames()
                .forEach(collName -> tableIds.add(TableId.tableId(db.getName(), collName)));
        LOG.info("\t list of available tables are: {}", tableIds);
        return tableIds;
    }

    public static io.debezium.relational.TableId toDbzTableId(TableId tableId) {
        return new io.debezium.relational.TableId(
                tableId.getSchemaName(), null, tableId.getTableName());
    }

    public static RowType getRecordIdRowType() {
        return DataTypes.ROW(DataTypes.FIELD("_id", DataTypes.STRING().notNull()));
    }

    public static RowType getJsonSchemaRowType() {
        return DataTypes.ROW(
                DataTypes.FIELD("_id", DataTypes.STRING().notNull()),
                DataTypes.FIELD("_fullDocument", DataTypes.STRING()));
    }

    public static Schema getJsonSchema() {
        return Schema.newBuilder()
                .physicalColumn("_id", DataTypes.STRING().notNull())
                .physicalColumn("_fullDocument", DataTypes.STRING())
                .primaryKey(Collections.singletonList("_id"))
                .build();
    }
}
