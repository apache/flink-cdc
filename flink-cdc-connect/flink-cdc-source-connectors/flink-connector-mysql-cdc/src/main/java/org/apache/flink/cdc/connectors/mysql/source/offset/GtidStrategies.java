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

package org.apache.flink.cdc.connectors.mysql.source.offset;

/** Resolves GTID strategies for supported database dialects. */
public class GtidStrategies {

    /** Config sentinel: detect the dialect from "SELECT VERSION()" at startup. */
    public static final String AUTO = "auto";

    private static final GtidStrategy MYSQL = new MysqlGtidStrategy();
    private static final GtidStrategy MARIADB = new MariaDbGtidStrategy();

    private GtidStrategies() {}

    public static boolean isSupportedDialect(String dialect) {
        return MariaDbGtidStrategy.DIALECT.equalsIgnoreCase(dialect)
                || MysqlGtidStrategy.DIALECT.equalsIgnoreCase(dialect)
                || AUTO.equalsIgnoreCase(dialect);
    }

    public static GtidStrategy of(String dialect) {
        if (MariaDbGtidStrategy.DIALECT.equalsIgnoreCase(dialect)) {
            return MARIADB;
        }
        return MYSQL;
    }

    /**
     * Resolves the effective dialect from the configured value and the server's "VERSION()" banner.
     * A pinned "mysql/mariadb" wins outright; "auto" or any unrecognized value, including "null"
     * sniffs the version text, which carries the literal "MariaDB" on MariaDB servers and not on
     * MySQL. Default to "mysql".
     */
    public static String resolveDialect(String configured, String versionText) {
        if (MysqlGtidStrategy.DIALECT.equalsIgnoreCase(configured)) {
            return MysqlGtidStrategy.DIALECT;
        }
        if (MariaDbGtidStrategy.DIALECT.equalsIgnoreCase(configured)) {
            return MariaDbGtidStrategy.DIALECT;
        }

        if (versionText != null
                && versionText.toLowerCase().contains(MariaDbGtidStrategy.DIALECT)) {
            return MariaDbGtidStrategy.DIALECT;
        }
        return MysqlGtidStrategy.DIALECT;
    }

    /**
     * Recovers the strategy from the GTID text alone, so a stateless {@link BinlogOffset} can still
     * pick the correct semantics after a restore. MariaDB's GTID (domain-server-sequence) form has
     * no ':', which is exactly what distinguishes it from MySQL's form (uuid:interval).
     */
    public static GtidStrategy detect(String gtidText) {
        if (MARIADB.canParse(gtidText) && !MYSQL.canParse(gtidText)) {
            return MARIADB;
        }

        return MYSQL;
    }
}
