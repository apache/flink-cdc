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

package org.apache.flink.cdc.debezium.internal;

import org.apache.flink.util.TemporaryClassLoaderContext;

import io.debezium.bean.DefaultBeanRegistry;
import io.debezium.bean.StandardBeanNames;
import io.debezium.bean.spi.BeanRegistry;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.service.DefaultServiceRegistry;
import io.debezium.snapshot.SnapshotLockProvider;
import io.debezium.snapshot.SnapshotQueryProvider;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.snapshot.SnapshotterServiceProvider;

/**
 * Builds the {@link SnapshotterService} that Debezium 2.6 threads through every snapshot and
 * streaming change event source.
 *
 * <p>Debezium wires this up in {@code BaseSourceTask.registerServiceProviders}, which is part of
 * the Kafka Connect task lifecycle that Flink CDC does not run — Flink CDC drives the change event
 * sources directly. This builds an equivalent registry instead.
 *
 * <p>The registry is created here rather than reused from {@link
 * CommonConnectorConfig#getServiceRegistry()} because the snapshot providers resolve the
 * connector-specific {@code SnapshotQuery} and {@code SnapshotLock} implementations by calling
 * {@code Class.forName(config.getString("connector.class"))}. Flink CDC builds its Debezium
 * configuration itself and never sets that property, so the lookup would throw a {@link
 * NullPointerException}; the connector class is supplied explicitly instead.
 */
public class SnapshotterServiceFactory {

    private static final String CONNECTOR_CLASS_PROPERTY = "connector.class";

    private SnapshotterServiceFactory() {}

    /**
     * Creates the snapshotter service for the given connector configuration.
     *
     * @param connectorConfig the Debezium connector configuration; the snapshotter is resolved from
     *     its {@code snapshot.mode}
     * @param connectorClass the Debezium connector class, used to select the connector-specific
     *     snapshot query and lock implementations
     * @return the snapshotter service, never null
     */
    public static SnapshotterService create(
            CommonConnectorConfig connectorConfig, Class<?> connectorClass) {
        // Debezium 2.6 resolves the Snapshotter SPI through ServiceLoader, which uses the
        // thread context class loader. On a Flink task thread that may be a user code class
        // loader from a previous job attempt, which is already closed. Pin it to the loader
        // that loaded Debezium.
        try (TemporaryClassLoaderContext ignored =
                TemporaryClassLoaderContext.of(SnapshotterServiceProvider.class.getClassLoader())) {
            final Configuration configuration =
                    connectorConfig
                            .getConfig()
                            .edit()
                            .with(CONNECTOR_CLASS_PROPERTY, connectorClass.getName())
                            .build();

            final BeanRegistry beanRegistry = new DefaultBeanRegistry();
            beanRegistry.add(StandardBeanNames.CONNECTOR_CONFIG, connectorConfig);

            final DefaultServiceRegistry serviceRegistry =
                    new DefaultServiceRegistry(configuration, beanRegistry);
            serviceRegistry.registerServiceProvider(new SnapshotLockProvider());
            serviceRegistry.registerServiceProvider(new SnapshotQueryProvider());
            serviceRegistry.registerServiceProvider(new SnapshotterServiceProvider());

            return serviceRegistry.getService(SnapshotterService.class);
        }
    }
}
