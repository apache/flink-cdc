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

package org.apache.flink.cdc.connectors.fluss.source.reader;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;

import java.io.Serializable;
import java.time.Duration;
import java.util.UUID;

/** Context for a Fluss KV snapshot lease. */
public class LeaseContext implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final ConfigOption<String> SCAN_KV_SNAPSHOT_LEASE_ID =
            ConfigOptions.key("scan.kv.snapshot.lease.id")
                    .stringType()
                    .defaultValue(UUID.randomUUID().toString());

    private static final ConfigOption<Duration> SCAN_KV_SNAPSHOT_LEASE_DURATION =
            ConfigOptions.key("scan.kv.snapshot.lease.duration")
                    .durationType()
                    .defaultValue(Duration.ofDays(1));

    private final String kvSnapshotLeaseId;
    private final long kvSnapshotLeaseDurationMs;

    public LeaseContext(String kvSnapshotLeaseId, long kvSnapshotLeaseDurationMs) {
        this.kvSnapshotLeaseId = kvSnapshotLeaseId;
        this.kvSnapshotLeaseDurationMs = kvSnapshotLeaseDurationMs;
    }

    public static LeaseContext fromConf(ReadableConfig tableOptions) {
        return new LeaseContext(
                tableOptions.get(SCAN_KV_SNAPSHOT_LEASE_ID),
                tableOptions.get(SCAN_KV_SNAPSHOT_LEASE_DURATION).toMillis());
    }

    public String getKvSnapshotLeaseId() {
        return kvSnapshotLeaseId;
    }

    public long getKvSnapshotLeaseDurationMs() {
        return kvSnapshotLeaseDurationMs;
    }
}
