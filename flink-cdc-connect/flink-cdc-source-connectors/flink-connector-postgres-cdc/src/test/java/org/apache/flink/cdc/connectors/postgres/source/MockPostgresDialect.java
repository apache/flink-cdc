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

package org.apache.flink.cdc.connectors.postgres.source;

import org.apache.flink.cdc.connectors.base.source.meta.offset.Offset;
import org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceConfig;
import org.apache.flink.util.Preconditions;

import java.util.function.Consumer;

/** Mock postgres dialect used to test changelog when checkpoint. */
public class MockPostgresDialect extends PostgresDialect {

    private static Consumer<Long> callback = null;

    public MockPostgresDialect(PostgresSourceConfig sourceConfig) {
        super(sourceConfig);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId, Offset offset) throws Exception {
        if (callback != null) {
            callback.accept(checkpointId);
        }
        super.notifyCheckpointComplete(checkpointId, offset);
    }

    public static void setNotifyCheckpointCompleteCallback(Consumer<Long> callback) {
        MockPostgresDialect.callback = Preconditions.checkNotNull(callback);
    }
}
