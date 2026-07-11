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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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
 * <p>The underlying {com.github.shyiko} binlog library already parses MariaDB GTID text ({@link
 * MariadbGtidSet#isMariaGtidSet(String)}), but its equals()/isContainedWithin() compare the server
 * id too, so the domain-keyed comparison is implemented here directly.
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

    @Override
    public boolean isContainedWithin(String sub, String sup) {
        Map<Long, Long> supSequence = toDomainSequence(sup);
        return toDomainSequence(sub).entrySet().stream()
                .allMatch(
                        entry -> supSequence.getOrDefault(entry.getKey(), -1L) >= entry.getValue());
    }

    @Override
    public String fixRestoredGtidSet(String server, String restored) {
        Map<Long, Long> serverSeq = toDomainSequence(server);
        // Preserver the restored tuples (domain-server-sequence); only lower the sequence where the
        // server is behind for that domain. Domains absent from the server are kept verbatim.
        // The server id from the restored tuple is preserved failover correctness keys on the
        // domain.
        StringBuilder sb = new StringBuilder();
        for (String tuple : splitTuples(restored)) {
            String[] parts = tuple.split("-");
            if (parts.length != 3) {
                throw new IllegalArgumentException("Invalid MariaDB GTID: " + tuple);
            }
            long domain = Long.parseLong(parts[0].trim());
            long serverId = Long.parseLong(parts[1].trim());
            long sequence = Long.parseLong(parts[2].trim());

            Long cap = serverSeq.get(domain);
            long effective = (cap != null) ? Math.min(sequence, cap) : sequence;
            if (sb.length() > 0) {
                sb.append(",");
            }
            sb.append(domain).append("-").append(serverId).append("-").append(effective);
        }

        return sb.toString();
    }

    private static List<String> splitTuples(String gtidText) {
        if (StringUtils.isBlank(gtidText)) {
            return new ArrayList<>();
        }

        return Arrays.stream(gtidText.split(","))
                .map(String::trim)
                .filter(StringUtils::isNotBlank)
                .collect(Collectors.toList());
    }

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
