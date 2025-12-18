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

package org.apache.flink.cdc.connectors.gaussdb.source;

import org.apache.flink.cdc.common.annotation.Experimental;

/**
 * The GaussDB CDC Source based on FLIP-27 and Incremental Snapshot Framework.
 *
 * <pre>{@code
 * GaussDBSource.<String>builder()
 *     .hostname("localhost")
 *     .port(8000)
 *     .database("mydb")
 *     .schemaList("public")
 *     .tableList("public.users")
 *     .username("gaussdb")
 *     .password("password")
 *     .slotName("flink_slot")
 *     .decodingPluginName("mppdb_decoding")
 *     .deserializer(new JsonDebeziumDeserializationSchema())
 *     .build();
 * }</pre>
 *
 * <p>See {@link GaussDBSourceBuilder} for more details.
 */
@Experimental
public class GaussDBSource {

    /**
     * Get a GaussDBSourceBuilder to build a {@link GaussDBSourceBuilder.GaussDBIncrementalSource}.
     *
     * @return a GaussDB source builder.
     */
    public static <T> GaussDBSourceBuilder<T> builder() {
        return GaussDBSourceBuilder.GaussDBIncrementalSource.builder();
    }
}
