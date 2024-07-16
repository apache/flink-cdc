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

package org.apache.flink.cdc.cli.guard;

import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.exceptions.GuardVerificationException;

import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlConformanceEnum;

/** Verification guard for common fields. */
public class CommonDefGuard {
    public static void verifyTableQualifier(String rawTableQualifier)
            throws GuardVerificationException {
        String tableQualifier = rawTableQualifier.replace("\\.", "A");
        if (tableQualifier.isEmpty()) {
            throw new GuardVerificationException(tableQualifier, "Empty table qualifier.");
        }
        if (tableQualifier.charAt(0) == '.') {
            throw new GuardVerificationException(
                    rawTableQualifier,
                    "Dot (.) was used as delimiter between schema and table name, which should not present at the beginning. Other usages in RegExp should be escaped with backslash (\\).");
        }
        if (tableQualifier.charAt(tableQualifier.length() - 1) == '.') {
            throw new GuardVerificationException(
                    rawTableQualifier,
                    "Dot (.) was used as delimiter between schema and table name, which should not present at the end. Other usages in RegExp should be escaped with backslash (\\).");
        }

        int delimiterDotCount = 0;
        for (int i = 0; i < tableQualifier.length(); i++) {
            // Accessing charAt(i - 1) is safe here
            // since tableQualifier[0] can't be '.'
            if (tableQualifier.charAt(i) == '.') {
                delimiterDotCount++;
            }
        }

        // delimiter dot must present exactly once
        if (delimiterDotCount > 2) {
            throw new GuardVerificationException(
                    rawTableQualifier,
                    "Dot (.) was used as delimiter between schema and table name. Other usages in RegExp should be escaped with backslash (\\).");
        }

        try {
            TableId.parse(tableQualifier);
        } catch (IllegalArgumentException e) {
            throw new GuardVerificationException(
                    rawTableQualifier, "Illegal table qualifier " + tableQualifier);
        }
    }

    public static SqlParser getCalciteParser(String sql) {
        return SqlParser.create(
                sql,
                SqlParser.Config.DEFAULT
                        .withConformance(SqlConformanceEnum.MYSQL_5)
                        .withCaseSensitive(true)
                        .withLex(Lex.JAVA));
    }
}
