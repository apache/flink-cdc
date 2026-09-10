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

import io.debezium.connector.mysql.GtidSet;

/** MySQL UUID-and-interval GTID strategy. */
public class MysqlGtidStrategy implements GtidStrategy {

    private static final long seriaVersionUID = 1L;

    public static final String DIALECT = "mysql";

    @Override
    public String dialect() {
        return DIALECT;
    }

    @Override
    public boolean canParse(String gtidText) {
        if (gtidText == null) {
            return false;
        }

        try {
            new GtidSet(gtidText);

            // MySQL GTID sets are uuid:interval; the ':' separator distinguishes them from
            // MariaDB's domain-server-sequence form, which GtidSet would silently mis-parse.
            return gtidText.isBlank() || gtidText.contains(":");
        } catch (RuntimeException e) {
            return false;
        }
    }

    @Override
    public boolean isEqual(String a, String b) {
        return new GtidSet(a).equals(new GtidSet(b));
    }

    @Override
    public boolean isContainedWithin(String sub, String sup) {
        return new GtidSet(sub).isContainedWithin(new GtidSet(sup));
    }
}
