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

package org.apache.flink.cdc.connectors.oceanbase.source.converter;

import org.apache.flink.cdc.debezium.table.DeserializationRuntimeConverter;
import org.apache.flink.cdc.debezium.table.DeserializationRuntimeConverterFactory;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.LogicalType;

import org.apache.kafka.connect.data.Schema;

import java.time.ZoneId;
import java.util.Optional;

/** Used to create {@link DeserializationRuntimeConverterFactory} specified to OceanBase. */
public class OceanBaseDeserializationConverterFactory {

    public static DeserializationRuntimeConverterFactory instance() {
        return new DeserializationRuntimeConverterFactory() {

            private static final long serialVersionUID = 1L;

            @Override
            public Optional<DeserializationRuntimeConverter> createUserDefinedConverter(
                    LogicalType logicalType, ZoneId serverTimeZone) {
                switch (logicalType.getTypeRoot()) {
                    case ARRAY:
                        return createArrayConverter();
                    default:
                        return Optional.empty();
                }
            }
        };
    }

    private static Optional<DeserializationRuntimeConverter> createArrayConverter() {
        return Optional.of(
                new DeserializationRuntimeConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object convert(Object dbzObj, Schema schema) throws Exception {
                        if (dbzObj instanceof String) {
                            String[] enums = ((String) dbzObj).split(",");
                            StringData[] elements = new StringData[enums.length];
                            for (int i = 0; i < enums.length; i++) {
                                elements[i] = StringData.fromString(enums[i]);
                            }
                            return new GenericArrayData(elements);
                        }
                        throw new IllegalArgumentException(
                                String.format(
                                        "Unable convert to Flink ARRAY type from unexpected value '%s'",
                                        dbzObj));
                    }
                });
    }
}
