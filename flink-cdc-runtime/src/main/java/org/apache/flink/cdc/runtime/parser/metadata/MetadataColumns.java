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

package org.apache.flink.cdc.runtime.parser.metadata;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;

import java.util.Arrays;
import java.util.List;

/** Contains all supported metadata columns that could be used in transform expressions. */
public class MetadataColumns {
    public static final String DEFAULT_NAMESPACE_NAME = "__namespace_name__";
    public static final String DEFAULT_SCHEMA_NAME = "__schema_name__";
    public static final String DEFAULT_TABLE_NAME = "__table_name__";
    public static final String DEFAULT_DATA_EVENT_TYPE = "__data_event_type__";

    public static final List<Tuple3<String, DataType, Class<?>>> METADATA_COLUMNS =
            Arrays.asList(
                    Tuple3.of(DEFAULT_NAMESPACE_NAME, DataTypes.STRING(), String.class),
                    Tuple3.of(DEFAULT_SCHEMA_NAME, DataTypes.STRING(), String.class),
                    Tuple3.of(DEFAULT_TABLE_NAME, DataTypes.STRING(), String.class),
                    Tuple3.of(DEFAULT_DATA_EVENT_TYPE, DataTypes.STRING(), String.class));
}
