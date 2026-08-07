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

package org.apache.flink.cdc.connectors.mysql.source.parser;

import io.debezium.connector.mysql.antlr.MySqlAntlrDdlParser;
import io.debezium.connector.mysql.antlr.listener.CreateTableParserListener;
import io.debezium.ddl.parser.mysql.generated.MySqlParser;
import org.antlr.v4.runtime.tree.ParseTreeListener;

import java.util.List;

/**
 * Handles regular CREATE TABLE statements while leaving CREATE TABLE ... LIKE processing to {@link
 * CustomAlterTableParserListener}.
 *
 * <p>The custom listener owns both the Debezium table cache update and the Flink CDC schema event.
 * Keeping that operation in one listener lets it preserve MySQL's no-op semantics for {@code IF NOT
 * EXISTS} without the Debezium listener overwriting the target schema first.
 */
final class CustomCreateTableParserListener extends CreateTableParserListener {

    CustomCreateTableParserListener(MySqlAntlrDdlParser parser, List<ParseTreeListener> listeners) {
        super(parser, listeners);
    }

    /**
     * Copy-table statements are handled atomically by {@link CustomAlterTableParserListener}.
     * Calling the parent implementation here would update the shared schema cache before the custom
     * listener can determine whether MySQL treated the statement as a no-op.
     */
    @Override
    public void exitCopyCreateTable(MySqlParser.CopyCreateTableContext ctx) {
        // Intentionally empty.
    }
}
