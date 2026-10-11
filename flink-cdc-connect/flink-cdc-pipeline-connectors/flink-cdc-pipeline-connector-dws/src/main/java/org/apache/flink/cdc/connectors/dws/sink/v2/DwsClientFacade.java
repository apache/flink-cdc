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

package org.apache.flink.cdc.connectors.dws.sink.v2;

import org.apache.flink.cdc.connectors.dws.sink.DwsClientConfigFactory;
import org.apache.flink.cdc.connectors.dws.sink.DwsDataSinkConfig;

import com.huaweicloud.dws.client.DwsClient;
import com.huaweicloud.dws.client.DwsConfig;
import com.huaweicloud.dws.client.exception.DwsClientException;
import com.huaweicloud.dws.client.model.TableName;
import com.huaweicloud.dws.client.op.Operate;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/** Minimal boundary around the official DWS client used by the sink writer. */
interface DwsClientFacade extends AutoCloseable {

    void write(String tableName, Map<String, Object> values) throws IOException;

    void delete(String tableName, Map<String, Object> values) throws IOException;

    void flush() throws IOException;

    default void refreshTableSchema(String tableName) throws IOException {}

    default void removeTableSchema(String tableName) throws IOException {}

    default Throwable asyncFailure() {
        return null;
    }

    @Override
    void close() throws IOException;

    /** Lazily constructs one official client for one sink writer. */
    final class Official implements DwsClientFacade {
        private final DwsDataSinkConfig settings;
        private final AtomicReference<Throwable> firstAsyncFailure = new AtomicReference<>();
        private DwsClient client;

        Official(DwsDataSinkConfig settings) {
            this.settings = settings;
        }

        @Override
        public void write(String tableName, Map<String, Object> values) throws IOException {
            try {
                submit(client().write(tableName), values, "write", tableName);
            } catch (DwsClientException e) {
                throw new IOException("Failed to prepare DWS write for table " + tableName, e);
            }
        }

        @Override
        public void delete(String tableName, Map<String, Object> values) throws IOException {
            try {
                submit(client().delete(tableName), values, "delete", tableName);
            } catch (DwsClientException e) {
                throw new IOException("Failed to prepare DWS delete for table " + tableName, e);
            }
        }

        @Override
        public void flush() throws IOException {
            if (client == null) {
                return;
            }
            try {
                client.flush();
            } catch (DwsClientException e) {
                throw new IOException("Failed to flush the official DWS client.", e);
            }
        }

        @Override
        public void refreshTableSchema(String tableName) throws IOException {
            try {
                DwsClient currentClient = client();
                TableName parsedTableName = TableName.valueOf(tableName);
                currentClient.removeTableSchema(parsedTableName);
                currentClient.getTableSchema(parsedTableName);
            } catch (DwsClientException | RuntimeException e) {
                throw new IOException("Failed to reload DWS schema for table " + tableName, e);
            }
        }

        @Override
        public void removeTableSchema(String tableName) throws IOException {
            if (client == null) {
                return;
            }
            try {
                client.removeTableSchema(TableName.valueOf(tableName));
            } catch (RuntimeException e) {
                throw new IOException("Failed to remove DWS schema for table " + tableName, e);
            }
        }

        @Override
        public Throwable asyncFailure() {
            return firstAsyncFailure.get();
        }

        @Override
        public void close() throws IOException {
            if (client != null) {
                client.close();
                client = null;
            }
        }

        private DwsClient client() throws IOException {
            if (client == null) {
                try {
                    DwsConfig config =
                            DwsClientConfigFactory.create(settings)
                                    .onError(
                                            (failure, ignoredClient) -> {
                                                firstAsyncFailure.compareAndSet(null, failure);
                                                return null;
                                            });
                    client = new DwsClient(config);
                } catch (RuntimeException e) {
                    throw new IOException("Failed to initialize the official DWS client.", e);
                }
            }
            return client;
        }

        private static void submit(
                Operate operation,
                Map<String, Object> values,
                String operationName,
                String tableName)
                throws IOException {
            try {
                for (Map.Entry<String, Object> entry : values.entrySet()) {
                    operation.setObject(entry.getKey(), entry.getValue());
                }
                operation.commit();
            } catch (DwsClientException | RuntimeException e) {
                throw new IOException(
                        String.format(
                                "Failed to submit DWS %s for table %s.", operationName, tableName),
                        e);
            }
        }
    }
}
