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
import org.apache.flink.cdc.composer.definition.RouteDef;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

import static org.apache.flink.cdc.cli.guard.CommonDefGuard.verifyTableQualifier;

/** Syntax guard for route definition. */
public class RouteDefGuard {
    private static final Logger LOG = LoggerFactory.getLogger(RouteDefGuard.class);

    public static void verify(RouteDef routeDef) throws GuardVerificationException {
        LOG.info("Verifying route definition {}", routeDef);

        Optional<String> replaceSymbol = routeDef.getReplaceSymbol();

        verifyTableQualifier(routeDef.getSourceTable());
        if (replaceSymbol.isPresent()) {
            if (!routeDef.getSinkTable().contains(replaceSymbol.get())) {
                throw new GuardVerificationException(
                        routeDef,
                        "Replace symbol is specified but not present in sink definition.");
            }
            verifyTableQualifier(
                    routeDef.getSinkTable().replace(replaceSymbol.get(), "SomeValidTableName"));
        } else {
            verifyTableQualifier(routeDef.getSinkTable());
        }
    }
}
