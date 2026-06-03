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

package org.apache.flink.cdc.connectors.tdengine.utils;

import java.sql.SQLException;
import java.util.Locale;

/** Retry helpers for TDengine JDBC failures. */
public class TDengineRetryUtils {

    private TDengineRetryUtils() {}

    public static boolean isTransient(SQLException failure) {
        for (SQLException current = failure;
                current != null;
                current = current.getNextException()) {
            String sqlState = current.getSQLState();
            if (sqlState != null && sqlState.startsWith("08")) {
                return true;
            }
            String message = current.getMessage();
            if (message == null) {
                continue;
            }
            String lower = message.toLowerCase(Locale.ROOT);
            if (lower.contains("connection")
                    || lower.contains("timeout")
                    || lower.contains("temporarily")
                    || lower.contains("network")
                    || lower.contains("broken pipe")
                    || lower.contains("refused")) {
                return true;
            }
        }
        return false;
    }
}
