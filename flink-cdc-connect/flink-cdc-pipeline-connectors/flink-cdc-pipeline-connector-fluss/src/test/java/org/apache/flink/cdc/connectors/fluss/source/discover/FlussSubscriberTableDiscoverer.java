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

package org.apache.flink.cdc.connectors.fluss.source.discover;

import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.source.discover.TableDiscoverer;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.batch.BatchScanUtils;
import org.apache.fluss.client.table.scanner.batch.BatchScanner;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataTypeRoot;
import org.apache.fluss.types.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * A test-only {@link TableDiscoverer} that reads the list of subscribed tables from a Fluss
 * <b>primary-key table</b> using a bounded {@code LIMIT} batch scan (no log subscription).
 *
 * <p><b>Expected schema:</b> the subscription table must place the fully-qualified table name
 * (formatted as {@code "database.tableName"}) in its <b>first column</b>. For example:
 *
 * <pre>{@code
 * CREATE TABLE subscription_list (
 *     table_name STRING PRIMARY KEY NOT ENFORCED
 * );
 * INSERT INTO subscription_list VALUES ('source_db.orders'), ('source_db.products');
 * }</pre>
 *
 * <p>The subscriber issues a {@code table.newScan().limit(limit).createBatchScanner(bucket)} on
 * every bucket of the subscription table and collects the rows via {@link
 * BatchScanUtils#collectAllRows(List)}. At most {@code limit} rows per bucket are returned.
 */
public class FlussSubscriberTableDiscoverer implements TableDiscoverer {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(FlussSubscriberTableDiscoverer.class);

    /** The fully-qualified path of the subscription table, e.g. {@code "meta_db.subscription"}. */
    private final TablePath subscriptionTablePath;

    /** Maximum number of rows per bucket to read from the subscription table. */
    private final int limit;

    private transient Connection connection;

    private FlussSubscriberTableDiscoverer(TablePath subscriptionTablePath, int limit) {
        if (limit <= 0) {
            throw new IllegalArgumentException(
                    "FlussTableSubscriber limit must be positive, got " + limit);
        }
        this.subscriptionTablePath = subscriptionTablePath;
        this.limit = limit;
    }

    public FlussSubscriberTableDiscoverer(String fullyQualifiedTableName, int limit) {
        this(parseTablePath(fullyQualifiedTableName), limit);
    }

    @Override
    public void open(Context context) throws Exception {
        org.apache.flink.cdc.common.configuration.Configuration config = context.getConfiguration();
        String bootstrapServers =
                config.get(
                        org.apache.flink.cdc.common.configuration.ConfigOptions.key(
                                        "bootstrap.servers")
                                .stringType()
                                .noDefaultValue()
                                .withDescription("Fluss bootstrap servers."));
        if (bootstrapServers == null || bootstrapServers.isEmpty()) {
            throw new IllegalArgumentException(
                    "'bootstrap.servers' is required for FlussTableSubscriber.");
        }
        org.apache.fluss.config.Configuration flussConfig =
                new org.apache.fluss.config.Configuration();
        flussConfig.setString(
                org.apache.fluss.config.ConfigOptions.BOOTSTRAP_SERVERS.key(), bootstrapServers);
        connection = ConnectionFactory.createConnection(flussConfig);
    }

    @Override
    public Set<TableId> discover() throws Exception {
        Set<TableId> result = new LinkedHashSet<>();
        try (Table table = connection.getTable(subscriptionTablePath)) {
            TableInfo tableInfo = table.getTableInfo();
            validateSchema(tableInfo.getRowType());
            long tableId = tableInfo.getTableId();
            int numBuckets = tableInfo.getNumBuckets();

            List<BatchScanner> scanners = new ArrayList<>(numBuckets);
            try {
                for (int bucketId = 0; bucketId < numBuckets; bucketId++) {
                    scanners.add(
                            table.newScan()
                                    .limit(limit)
                                    .createBatchScanner(new TableBucket(tableId, bucketId)));
                }
                List<InternalRow> rows = BatchScanUtils.collectAllRows(scanners);
                for (InternalRow row : rows) {
                    if (row == null || row.isNullAt(0)) {
                        continue;
                    }
                    String fqn = row.getString(0).toString();
                    TableId parsed = safeParseTableId(fqn);
                    if (parsed != null) {
                        result.add(parsed);
                    }
                }
            } finally {
                for (BatchScanner scanner : scanners) {
                    try {
                        scanner.close();
                    } catch (Exception e) {
                        LOG.warn(
                                "Failed to close batch scanner for subscription table {}.",
                                subscriptionTablePath,
                                e);
                    }
                }
            }
        }
        LOG.info(
                "FlussTableSubscriber discovered {} tables from subscription table {} (limit={}).",
                result.size(),
                subscriptionTablePath,
                limit);
        return result;
    }

    @Override
    public void close() throws Exception {
        if (connection != null) {
            connection.close();
        }
    }

    public TablePath getSubscriptionTablePath() {
        return subscriptionTablePath;
    }

    public int getLimit() {
        return limit;
    }

    /**
     * Enforces the contract that the subscription table has exactly one column of type STRING (the
     * fully-qualified target table name).
     */
    private void validateSchema(RowType rowType) {
        int fieldCount = rowType.getFieldCount();
        if (fieldCount != 1) {
            throw new IllegalStateException(
                    "FlussTableSubscriber requires the subscription table '"
                            + subscriptionTablePath
                            + "' to have exactly one column, but found "
                            + fieldCount
                            + " columns: "
                            + rowType.getFieldNames()
                            + ". The single column must be of type STRING and contain the"
                            + " fully-qualified 'database.tableName' of each subscribed table.");
        }
        DataTypeRoot rootType = rowType.getTypeAt(0).getTypeRoot();
        if (rootType != DataTypeRoot.STRING && rootType != DataTypeRoot.CHAR) {
            throw new IllegalStateException(
                    "FlussTableSubscriber requires the single column of subscription table '"
                            + subscriptionTablePath
                            + "' to be of type STRING, but found type '"
                            + rowType.getTypeAt(0).asSummaryString()
                            + "' for column '"
                            + rowType.getFieldNames().get(0)
                            + "'.");
        }
    }

    private static TablePath parseTablePath(String fqn) {
        TablePath parsed = safeParseFlussTablePath(fqn);
        if (parsed == null) {
            throw new IllegalArgumentException(
                    "Invalid fully-qualified table name '"
                            + fqn
                            + "'. Expected format: 'database.tableName'.");
        }
        return parsed;
    }

    private static TablePath safeParseFlussTablePath(String fqn) {
        if (fqn == null) {
            return null;
        }
        int dot = fqn.indexOf('.');
        if (dot <= 0 || dot == fqn.length() - 1) {
            return null;
        }
        return new TablePath(fqn.substring(0, dot), fqn.substring(dot + 1));
    }

    private static TableId safeParseTableId(String fqn) {
        if (fqn == null) {
            return null;
        }
        int dot = fqn.indexOf('.');
        if (dot <= 0 || dot == fqn.length() - 1) {
            return null;
        }
        return TableId.tableId(fqn.substring(0, dot), fqn.substring(dot + 1));
    }
}
