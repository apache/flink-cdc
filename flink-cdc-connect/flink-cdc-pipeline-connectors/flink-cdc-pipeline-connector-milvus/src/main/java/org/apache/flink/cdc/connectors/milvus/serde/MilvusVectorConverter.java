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

package org.apache.flink.cdc.connectors.milvus.serde;

import org.apache.flink.cdc.connectors.milvus.utils.MilvusVectorFieldSpec;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

import java.util.ArrayList;
import java.util.List;

/** Converts source vector values to Milvus dense float vectors. */
public class MilvusVectorConverter {

    private MilvusVectorConverter() {}

    public static List<Float> toFloatVector(Object value, MilvusVectorFieldSpec spec) {
        if (value == null) {
            throw new IllegalArgumentException(
                    "Vector field " + spec.getFieldName() + " must not be null.");
        }
        List<Float> vector;
        if (value instanceof List) {
            vector = fromList((List<?>) value);
        } else if (value instanceof String) {
            vector = fromJsonString((String) value, spec);
        } else {
            throw new IllegalArgumentException(
                    "Vector field "
                            + spec.getFieldName()
                            + " must be a list or JSON array string but was "
                            + value.getClass().getName()
                            + ".");
        }
        if (vector.size() != spec.getDimension()) {
            throw new IllegalArgumentException(
                    "Vector field "
                            + spec.getFieldName()
                            + " dimension mismatch. Expected "
                            + spec.getDimension()
                            + " but was "
                            + vector.size()
                            + ".");
        }
        return vector;
    }

    private static List<Float> fromList(List<?> values) {
        List<Float> vector = new ArrayList<>(values.size());
        for (Object value : values) {
            vector.add(toFiniteFloat(value));
        }
        return vector;
    }

    private static List<Float> fromJsonString(String value, MilvusVectorFieldSpec spec) {
        JsonElement element = JsonParser.parseString(value);
        if (!element.isJsonArray()) {
            throw new IllegalArgumentException(
                    "Vector field " + spec.getFieldName() + " JSON value must be an array.");
        }
        JsonArray array = element.getAsJsonArray();
        List<Float> vector = new ArrayList<>(array.size());
        for (JsonElement item : array) {
            if (!item.isJsonPrimitive() || !item.getAsJsonPrimitive().isNumber()) {
                throw new IllegalArgumentException(
                        "Vector field " + spec.getFieldName() + " contains a non-numeric item.");
            }
            vector.add(toFiniteFloat(item.getAsFloat()));
        }
        return vector;
    }

    private static Float toFiniteFloat(Object value) {
        if (!(value instanceof Number)) {
            throw new IllegalArgumentException("Vector values must be numeric.");
        }
        float item = ((Number) value).floatValue();
        if (!Float.isFinite(item)) {
            throw new IllegalArgumentException("Vector values must be finite numbers.");
        }
        return item;
    }
}
