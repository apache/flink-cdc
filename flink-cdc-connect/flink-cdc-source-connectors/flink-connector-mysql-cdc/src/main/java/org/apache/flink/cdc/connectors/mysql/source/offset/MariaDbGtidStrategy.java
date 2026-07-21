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

import com.github.shyiko.mysql.binlog.MariadbGtidSet;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * MariaDB (domain-server-sequence) GTID strategy.
 *
 * <p>Unlike MySQL's (uuid:interval) model, a MariaDB GTID identifies a position by (domain, server,
 * sequence) where the sequence is monotonically increasing per domain across servers. On
 * master/replica failover the server id changes but the same domain keeps advancing the sequence,
 * so comparison must be keyed on the domain and ignore the server id - otherwise a post-failover
 * event looks like a different stream and the offset is mishandled (Debezium dbz#1672, Flink CDC
 * issue #2929).
 *
 * <p>The underlying {@code com.github.shyiko} binlog library already parses MariaDB GTID text
 * ({@link MariadbGtidSet#isMariaGtidSet(String)}), but its {@code equals()} and {@code
 * isContainedWithin()} methods compare the server ID too. The domain-keyed comparison is therefore
 * implemented here directly.
 */
public class MariaDbGtidStrategy implements GtidStrategy {

    private static final long seriaVersionUID = 1L;

    public static final String DIALECT = "mariadb";

    @Override
    public String dialect() {
        return DIALECT;
    }

    @Override
    public boolean canParse(String gtidText) {
        return gtidText != null && MariadbGtidSet.isMariaGtidSet(gtidText.trim());
    }

    @Override
    public boolean isEqual(String a, String b) {
        return toDomainSequence(a).equals(toDomainSequence(b));
    }

    /**
     * MariaDB GTIDs have the form {@code domain-server-sequence}. A primary failover may change the
     * server ID while the same domain continues advancing. Containment is therefore evaluated by
     * {@code domain + sequence}; the server ID is deliberately ignored. For example, {@code
     * 1-100-500} is contained within {@code 1-101-520}, but {@code 1-100-521} is not. If a domain
     * present in {@code sub} is absent from {@code sup}, containment is also false.
     */
    @Override
    public boolean isContainedWithin(String sub, String sup) {
        Map<Long, Long> supSequence = toDomainSequence(sup);
        return toDomainSequence(sub).entrySet().stream()
                .allMatch(
                        entry -> supSequence.getOrDefault(entry.getKey(), -1L) >= entry.getValue());
    }

    /** Converts GTID text into the highest observed sequence for each replication domain. */
    private static Map<Long, Long> toDomainSequence(String gtidText) {
        if (StringUtils.isBlank(gtidText)) {
            return new HashMap<>();
        }

        Map<Long, Long> domainToSeq = new HashMap<>();
        for (String tuple : gtidText.split(",")) {
            String trimmedTuple = tuple.trim();
            if (StringUtils.isBlank(trimmedTuple)) {
                continue;
            }

            String[] parts = trimmedTuple.split("-");
            if (parts.length != 3) {
                throw new IllegalArgumentException("Invalid MariaDB gtid format: " + trimmedTuple);
            }

            Long domain = Long.parseLong(parts[0].trim());
            Long sequence = Long.parseLong(parts[2].trim());
            domainToSeq.merge(domain, sequence, Math::max);
        }
        return domainToSeq;
    }
}
