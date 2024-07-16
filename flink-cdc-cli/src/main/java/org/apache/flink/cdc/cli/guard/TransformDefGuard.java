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

import org.apache.flink.cdc.common.exceptions.GuardVerificationException;
import org.apache.flink.cdc.composer.definition.TransformDef;

import org.apache.flink.shaded.guava31.com.google.common.base.Strings;

import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

import static org.apache.flink.cdc.cli.guard.CommonDefGuard.getCalciteParser;
import static org.apache.flink.cdc.cli.guard.CommonDefGuard.verifyTableQualifier;

/** Syntax guard for transform definition. */
public class TransformDefGuard {

    private static final Logger LOG = LoggerFactory.getLogger(TransformDefGuard.class);

    public static void verify(TransformDef transformDef) throws GuardVerificationException {
        LOG.info("Verifying transform definition {}", transformDef);

        verifyTableQualifier(transformDef.getSourceTable());

        verifyKeys(transformDef.getPrimaryKeys());
        verifyKeys(transformDef.getPartitionKeys());

        transformDef.getProjection().ifPresent(TransformDefGuard::verifyProjectionRule);
        transformDef.getFilter().ifPresent(TransformDefGuard::verifyFilterRule);
    }

    private static void verifyKeys(String keys) throws GuardVerificationException {
        if (Strings.isNullOrEmpty(keys)) {
            return;
        }
        Arrays.stream(keys.split(","))
                .map(String::trim)
                .forEach(TransformDefGuard::verifySqlIdentifier);
    }

    private static void verifyProjectionRule(String projectionRule)
            throws GuardVerificationException {
        try {
            SqlNode selectNode =
                    getCalciteParser("select " + projectionRule + " from tb").parseQuery();
            if (selectNode instanceof SqlSelect) {
                for (SqlNode node : ((SqlSelect) selectNode).getSelectList().getList()) {
                    if (node instanceof SqlBasicCall) {
                        SqlBasicCall call = (SqlBasicCall) node;
                        if (!(SqlKind.AS.equals(call.getOperator().getKind())
                                && call.getOperandList().size() == 2
                                && call.getOperandList().get(1) instanceof SqlIdentifier)) {
                            throw new GuardVerificationException(
                                    projectionRule,
                                    String.format(
                                            "%s is neither a column identifier nor an aliased expression. Expected: <Expression> AS <Identifier>",
                                            node));
                        }
                    } else if (!(node instanceof SqlIdentifier)) {
                        throw new GuardVerificationException(
                                projectionRule,
                                String.format(
                                        "%s is neither a column identifier nor an aliased expression. Expected: <Expression> AS <Identifier>",
                                        node));
                    }
                }
            }
        } catch (SqlParseException e) {
            throw new GuardVerificationException(
                    projectionRule, projectionRule + " is not a valid projection rule");
        }
    }

    private static void verifyFilterRule(String filterRule) throws GuardVerificationException {
        try {
            SqlNode selectNode =
                    getCalciteParser("select * from tb where " + filterRule).parseQuery();
        } catch (SqlParseException e) {
            throw new GuardVerificationException(
                    filterRule, filterRule + " is not a valid filter rule");
        }
    }

    private static void verifySqlIdentifier(String identifier) throws GuardVerificationException {
        try {
            SqlNode selectNode = getCalciteParser("select " + identifier + " from tb").parseQuery();
            if (selectNode instanceof SqlSelect) {
                if (!((SqlSelect) selectNode)
                        .getSelectList().getList().stream()
                                .allMatch(node -> node instanceof SqlIdentifier)) {
                    throw new GuardVerificationException(
                            identifier, identifier + " is not a valid SQL identifier");
                }
            } else {
                throw new GuardVerificationException(
                        identifier, identifier + " is not a valid SQL identifier");
            }
        } catch (SqlParseException e) {
            throw new GuardVerificationException(
                    identifier, identifier + " is not a valid SQL identifier");
        }
    }
}
