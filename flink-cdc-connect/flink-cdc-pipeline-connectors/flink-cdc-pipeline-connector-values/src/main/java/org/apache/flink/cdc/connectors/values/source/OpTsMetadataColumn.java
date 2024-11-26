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

package org.apache.flink.cdc.connectors.values.source;

import org.apache.flink.cdc.common.source.SupportedMetadataColumn;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;

import java.util.Map;

/** A {@link SupportedMetadataColumn} for op_ts. */
public class OpTsMetadataColumn implements SupportedMetadataColumn {

    @Override
    public String getName() {
        return "op_ts";
    }

    @Override
    public DataType getType() {
        return DataTypes.BIGINT();
    }

    @Override
    public Class<?> getJavaClass() {
        return Long.class;
    }

    @Override
    public Object read(Map<String, String> metadata) {
        if (metadata.containsKey(getName())) {
            return Long.parseLong(metadata.get(getName()));
        }
        return null;
    }
}
