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

package org.apache.flink.cdc.common.source.discover;

import org.apache.flink.cdc.common.annotation.PublicEvolving;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.event.TableId;

import java.io.Serializable;
import java.util.Set;

/**
 * Pluggable abstraction for discovering a set of tables that a source connector should read.
 * Implementations are loaded via SPI through {@link TableDiscovererFactory} and determine which
 * tables the source should subscribe to.
 *
 * <p>Lifecycle: {@link #open(Context)} is called once before the first call to {@link #discover()}.
 * {@link #close()} is called when the discoverer is no longer needed. Implementations manage their
 * own resources (e.g., connections) within this lifecycle.
 *
 * <p>Built-in implementations include:
 *
 * <ul>
 *   <li>{@link JdbcTableDiscoverer} - reads the subscription list from a JDBC database table.
 * </ul>
 */
@PublicEvolving
public interface TableDiscoverer extends Serializable, AutoCloseable {

    /**
     * Opens this discoverer and initializes any resources needed for table discovery.
     *
     * @param context The context providing configuration and class loader.
     * @throws Exception if initialization fails.
     */
    void open(Context context) throws Exception;

    /**
     * Discovers and returns the set of tables to subscribe to.
     *
     * @return A set of {@link TableId} representing the tables to read.
     * @throws Exception if the discovery fails.
     */
    Set<TableId> discover() throws Exception;

    /**
     * Closes this discoverer and releases any resources.
     *
     * @throws Exception if closing fails.
     */
    @Override
    void close() throws Exception;

    /** Context providing runtime information for the discoverer. */
    interface Context {

        /**
         * Returns the full connector configuration. Discoverer implementations read their own
         * configuration keys (e.g., {@code table.discoverer.jdbc.url}) directly from this
         * configuration.
         */
        Configuration getConfiguration();

        /** Returns the user code class loader of the current session. */
        ClassLoader getUserCodeClassLoader();
    }
}
