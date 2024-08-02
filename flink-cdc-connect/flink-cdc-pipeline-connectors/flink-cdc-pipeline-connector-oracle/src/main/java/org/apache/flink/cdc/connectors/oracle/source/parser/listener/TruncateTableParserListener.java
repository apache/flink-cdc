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

package org.apache.flink.cdc.connectors.oracle.source.parser.listener;

import io.debezium.connector.oracle.antlr.OracleDdlParser;
import io.debezium.ddl.parser.oracle.generated.PlSqlParser;
import io.debezium.relational.TableId;

/** This class is parsing Oracle truncate table statements. */
public class TruncateTableParserListener extends BaseParserListener {

    private final String catalogName;
    private final String schemaName;
    private final OracleDdlParser parser;

    public TruncateTableParserListener(
            final String catalogName, final String schemaName, final OracleDdlParser parser) {
        this.catalogName = catalogName;
        this.schemaName = schemaName;
        this.parser = parser;
    }

    @Override
    public void enterTruncate_table(final PlSqlParser.Truncate_tableContext ctx) {
        TableId tableId = new TableId(catalogName, schemaName, getTableName(ctx.tableview_name()));
        parser.signalTruncateTable(tableId, ctx);
        super.enterTruncate_table(ctx);
    }
}
