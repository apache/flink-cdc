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

import java.io.Serializable;

/** Dialect-specific operations for GTID parsing, comparison, and recovery. */
public interface GtidStrategy extends Serializable {

    /** Identifier of this dialect, e.g. "mysql" or "mariadb". */
    String dialect();

    boolean canParse(String gtidText);

    boolean isEqual(String a, String b);

    /** Whether the {@code sub} GTID set is fully contained within {@code sup}. */
    boolean isContainedWithin(String sub, String sup);

    /**
     * Caps the {@code restored} GTID set so it does not exceed what is currently available on the
     * server, returning the corrected GTID text. Mirrors the pre-abstraction call
     * GtidUtils.fixRestoredGtidSet for Mysql; for MariaDB it caps each domain's sequence at the
     * server's sequence for the domain.
     */
    String fixRestoredGtidSet(String server, String restored);
}
