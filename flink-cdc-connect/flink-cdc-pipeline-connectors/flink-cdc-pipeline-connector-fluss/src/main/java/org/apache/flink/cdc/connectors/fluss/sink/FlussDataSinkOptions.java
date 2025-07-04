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

package org.apache.flink.cdc.connectors.fluss.sink;

import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.common.configuration.ConfigOptions;

/** Options for {@link FlussDataSink}. */
public class FlussDataSinkOptions {

    // prefix for passing properties for fluss options.
    public static final String TABLE_PROPERTIES_PREFIX = "properties.table.";
    public static final String CLIENT_PROPERTIES_PREFIX = "properties.client.";

    public static final ConfigOption<String> BOOTSTRAP_SERVERS =
            ConfigOptions.key("bootstrap.servers")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The bootstrap servers for the Fluss sink connection.");

    public static final ConfigOption<String> BUCKET_KEY =
            ConfigOptions.key("bucket.key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Specific the distribution policy of each Fluss table. "
                                    + "Tables are separated by ';', and bucket keys are separated by ','. "
                                    + "Format: database1.table1:key1,key2;database1.table2:key3. \""
                                    + "Data will be distributed to each bucket according to the hash value of bucket-key (It must be a subset of the primary keys excluding partition keys of the primary key table). "
                                    + "If the table has a primary key and a bucket key is not specified, the bucket key will be used as primary key(excluding the partition key). "
                                    + "If the table has no primary key and the bucket key is not specified, "
                                    + "the data will be distributed to each bucket randomly.");

    public static final ConfigOption<String> BUCKET_NUMBER =
            ConfigOptions.key("bucket.num")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The number of buckets of each Fluss table."
                                    + "Tables are separated by ';'. "
                                    + "Format: database1.table1:4;database1.table2:8.");
}
