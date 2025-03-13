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

package org.apache.flink.cdc.common.shade.utils;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.util.Map;
import java.util.TimeZone;

import static org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationFeature.ACCEPT_EMPTY_ARRAY_AS_NULL_OBJECT;
import static org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES;
import static org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_AS_NULL;
import static org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.MapperFeature.REQUIRE_SETTERS_FOR_GETTERS;

/** {@link JsonNode} utility methods. */
public class JsonUtils {
    /** can use static singleton, inject: just make sure to reuse! */
    private static final ObjectMapper OBJECT_MAPPER =
            new ObjectMapper()
                    .configure(FAIL_ON_UNKNOWN_PROPERTIES, false)
                    .configure(ACCEPT_EMPTY_ARRAY_AS_NULL_OBJECT, true)
                    .configure(READ_UNKNOWN_ENUM_VALUES_AS_NULL, true)
                    .configure(REQUIRE_SETTERS_FOR_GETTERS, true)
                    .setTimeZone(TimeZone.getDefault())
                    // support java8 time api
                    .registerModule(new JavaTimeModule());

    private static final ObjectMapper DEFAULT_OBJECT_MAPPER = new ObjectMapper();

    public static Map<String, Object> toMap(JsonNode jsonNode) {
        return DEFAULT_OBJECT_MAPPER.convertValue(
                jsonNode, new TypeReference<Map<String, Object>>() {});
    }

    public static JsonNode toJsonNode(Object obj) {
        return OBJECT_MAPPER.valueToTree(obj);
    }
}
