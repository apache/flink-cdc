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

package org.apache.flink.cdc.connectors.kafka.utils;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.JsonFormatOptions;
import org.apache.flink.formats.json.JsonRowDataSerializationSchema;
import org.apache.flink.formats.json.RowDataToJsonConverters;
import org.apache.flink.table.types.logical.RowType;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.Arrays;

/**
 * Utils for creating JsonRowDataSerializationSchema.TODO: Remove this class after bump to Flink
 * 1.20 or higher.
 */
public class JsonRowDataSerializationSchemaUtils {

    /**
     * In flink>=1.20, the constructor of JsonRowDataSerializationSchema has 6 parameters, and in
     * flink<1.20, the constructor of JsonRowDataSerializationSchema has 5 parameters.
     */
    public static JsonRowDataSerializationSchema createSerializationSchema(
            RowType rowType,
            TimestampFormat timestampFormat,
            JsonFormatOptions.MapNullKeyMode mapNullKeyMode,
            String mapNullKeyLiteral,
            boolean encodeDecimalAsPlainNumber,
            boolean ignoreNullFields) {
        try {
            Class<?>[] fullParams =
                    new Class[] {
                        RowType.class,
                        TimestampFormat.class,
                        JsonFormatOptions.MapNullKeyMode.class,
                        String.class,
                        boolean.class,
                        boolean.class
                    };

            Object[] fullParamValues =
                    new Object[] {
                        rowType,
                        timestampFormat,
                        mapNullKeyMode,
                        mapNullKeyLiteral,
                        encodeDecimalAsPlainNumber,
                        ignoreNullFields
                    };

            for (int i = fullParams.length; i >= 5; i--) {
                try {
                    Constructor<?> constructor =
                            JsonRowDataSerializationSchema.class.getConstructor(
                                    Arrays.copyOfRange(fullParams, 0, i));

                    return (JsonRowDataSerializationSchema)
                            constructor.newInstance(Arrays.copyOfRange(fullParamValues, 0, i));
                } catch (NoSuchMethodException ignored) {
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to create JsonRowDataSerializationSchema,please check your Flink version is 1.19 or 1.20.",
                    e);
        }
        throw new RuntimeException(
                "Failed to find appropriate constructor for JsonRowDataSerializationSchema,please check your Flink version is 1.19 or 1.20.");
    }

    /**
     * In flink>=1.20, the constructor of RowDataToJsonConverters has 4 parameters, and in
     * flink<1.20, the constructor of RowDataToJsonConverters has 3 parameters.
     */
    public static RowDataToJsonConverters createRowDataToJsonConverters(
            TimestampFormat timestampFormat,
            JsonFormatOptions.MapNullKeyMode mapNullKeyMode,
            String mapNullKeyLiteral,
            boolean ignoreNullFields) {
        try {
            Class<?>[] fullParams =
                    new Class[] {
                        TimestampFormat.class,
                        JsonFormatOptions.MapNullKeyMode.class,
                        String.class,
                        boolean.class
                    };

            Object[] fullParamValues =
                    new Object[] {
                        timestampFormat, mapNullKeyMode, mapNullKeyLiteral, ignoreNullFields
                    };

            for (int i = fullParams.length; i >= 3; i--) {
                try {
                    Constructor<?> constructor =
                            RowDataToJsonConverters.class.getConstructor(
                                    Arrays.copyOfRange(fullParams, 0, i));

                    return (RowDataToJsonConverters)
                            constructor.newInstance(Arrays.copyOfRange(fullParamValues, 0, i));
                } catch (NoSuchMethodException ignored) {
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to create RowDataToJsonConverters,please check your Flink version is 1.19 or 1.20.",
                    e);
        }
        throw new RuntimeException(
                "Failed to find appropriate constructor for RowDataToJsonConverters,please check your Flink version is 1.19 or 1.20.");
    }

    /** flink>=1.20 only has the ENCODE_IGNORE_NULL_FIELDS parameter. */
    public static boolean enableIgnoreNullFields(ReadableConfig formatOptions) {
        try {
            Field field = JsonFormatOptions.class.getField("ENCODE_IGNORE_NULL_FIELDS");
            ConfigOption<Boolean> encodeOption = (ConfigOption<Boolean>) field.get(null);
            return formatOptions.get(encodeOption);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            return false;
        }
    }
}
