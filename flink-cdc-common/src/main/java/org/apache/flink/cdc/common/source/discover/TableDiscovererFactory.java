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

/**
 * Table-specific SPI factory that creates a {@link TableDiscoverer}. Implementations are discovered
 * at runtime via Java's {@link java.util.ServiceLoader} mechanism.
 *
 * <p>This is a compatibility specialization of {@link ObjectIdDiscovererFactory} whose discovered
 * object id type is {@link TableId}. The old {@link #identifier()} value now aliases {@link
 * #type()}.
 */
@PublicEvolving
public interface TableDiscovererFactory extends ObjectIdDiscovererFactory<TableId> {

    /** Compatibility alias for the old single identifier, now meaning storage-side type. */
    default String identifier() {
        return type();
    }

    @Override
    default Class<TableId> objectIdClass() {
        return TableId.class;
    }

    /** Creates a new uninitialized {@link TableDiscoverer}. */
    @Override
    TableDiscoverer createDiscoverer();

    /** Creates a discoverer context with the given configuration and class loader. */
    static ObjectIdDiscoverer.Context createContext(
            Configuration configuration, ClassLoader classLoader) {
        return ObjectIdDiscovererFactory.createContext(configuration, classLoader);
    }

    /** Discovers a table discoverer factory via SPI and delegates discoverer creation to it. */
    static TableDiscoverer createDiscoverer(String type, ClassLoader classLoader) {
        return (TableDiscoverer)
                ObjectIdDiscovererFactory.createDiscoverer(type, TableId.class, classLoader);
    }
}
