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

package org.apache.flink.cdc.connectors.starrocks.sink;

import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.utils.Preconditions;

import java.io.Serializable;

/** Configurations for schema change. */
public class SchemaChangeConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Timeout for a schema change on StarRocks side. */
    private final long timeoutSecond;

    public SchemaChangeConfig(long timeoutSecond) {
        Preconditions.checkArgument(
                timeoutSecond > 0, "Timeout must be positive, but actually is %s", timeoutSecond);
        this.timeoutSecond = timeoutSecond;
    }

    public long getTimeoutSecond() {
        return timeoutSecond;
    }

    public static SchemaChangeConfig from(Configuration config) {
        long timeoutSecond =
                Math.max(
                        1,
                        config.get(StarRocksDataSinkOptions.TABLE_SCHEMA_CHANGE_TIMEOUT)
                                .getSeconds());
        return new SchemaChangeConfig(timeoutSecond);
    }
}
