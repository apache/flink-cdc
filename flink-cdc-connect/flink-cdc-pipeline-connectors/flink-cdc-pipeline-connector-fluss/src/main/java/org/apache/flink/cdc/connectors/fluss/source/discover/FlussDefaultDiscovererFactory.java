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

import org.apache.flink.cdc.common.source.discover.TableDiscoverer;
import org.apache.flink.cdc.common.source.discover.TableDiscovererFactory;

/**
 * {@link TableDiscovererFactory} for {@link FlussDefaultDiscoverer}. Activated when {@code
 * table.discoverer.type = 'fluss-default'}.
 *
 * <p>This is the default discoverer for the Fluss source connector. It matches fully-qualified
 * Fluss table names against a user-provided regex pattern.
 */
public class FlussDefaultDiscovererFactory implements TableDiscovererFactory {

    public static final String TYPE = "fluss-default";

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public TableDiscoverer createDiscoverer() {
        return new FlussDefaultDiscoverer();
    }
}
