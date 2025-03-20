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

package org.apache.flink.cdc.common.configuration;

import org.apache.flink.cdc.common.utils.TimeUtils;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

/** Utility class for {@link Configuration} related helper functions. */
public class ConfigurationUtils {

    // --------------------------------------------------------------------------------------------
    //  Type conversion
    // --------------------------------------------------------------------------------------------

    /**
     * Tries to convert the raw value into the provided type.
     *
     * @param rawValue rawValue to convert into the provided type clazz
     * @param clazz clazz specifying the target type
     * @param <T> type of the result
     * @return the converted value if rawValue is of type clazz
     * @throws IllegalArgumentException if the rawValue cannot be converted in the specified target
     *     type clazz
     */
    @SuppressWarnings("unchecked")
    public static <T> T convertValue(Object rawValue, Class<?> clazz) {
        if (rawValue == null) {
            return null;
        }
        if (Integer.class.equals(clazz)) {
            return (T) convertToInt(rawValue);
        } else if (Long.class.equals(clazz)) {
            return (T) convertToLong(rawValue);
        } else if (Boolean.class.equals(clazz)) {
            return (T) convertToBoolean(rawValue);
        } else if (Float.class.equals(clazz)) {
            return (T) convertToFloat(rawValue);
        } else if (Double.class.equals(clazz)) {
            return (T) convertToDouble(rawValue);
        } else if (String.class.equals(clazz)) {
            return (T) convertToString(rawValue);
        } else if (clazz.isEnum()) {
            return (T) convertToEnum(rawValue, (Class<? extends Enum<?>>) clazz);
        } else if (clazz == Duration.class) {
            return (T) convertToDuration(rawValue);
        } else if (clazz == Map.class) {
            return (T) convertToProperties(rawValue);
        }

        throw new IllegalArgumentException("Unsupported type: " + clazz);
    }

    @SuppressWarnings("unchecked")
    static <T> T convertToList(Object rawValue, Class<?> atomicClass) {
        if (rawValue == null) {
            return null;
        } else if (rawValue instanceof List) {
            return (T) rawValue;
        } else {
            return (T)
                    StructuredOptionsSplitter.splitEscaped(rawValue.toString(), ';').stream()
                            .map(s -> convertValue(s, atomicClass))
                            .collect(Collectors.toList());
        }
    }

    @SuppressWarnings("unchecked")
    static Map<String, String> convertToProperties(Object o) {
        if (o == null) {
            return null;
        } else if (o instanceof Map) {
            return (Map<String, String>) o;
        } else {
            List<String> listOfRawProperties =
                    StructuredOptionsSplitter.splitEscaped(o.toString(), ',');
            return listOfRawProperties.stream()
                    .map(s -> StructuredOptionsSplitter.splitEscaped(s, ':'))
                    .peek(
                            pair -> {
                                if (pair.size() != 2) {
                                    throw new IllegalArgumentException(
                                            "Map item is not a key-value pair (missing ':'?)");
                                }
                            })
                    .collect(Collectors.toMap(a -> a.get(0), a -> a.get(1)));
        }
    }

    @SuppressWarnings("unchecked")
    public static <E extends Enum<?>> E convertToEnum(Object o, Class<E> clazz) {
        if (o == null) {
            return null;
        } else if (o.getClass().equals(clazz)) {
            return (E) o;
        }

        return Arrays.stream(clazz.getEnumConstants())
                .filter(
                        e ->
                                e.toString()
                                        .toUpperCase(Locale.ROOT)
                                        .equals(o.toString().toUpperCase(Locale.ROOT)))
                .findAny()
                .orElseThrow(
                        () ->
                                new IllegalArgumentException(
                                        String.format(
                                                "Could not parse value for enum %s. Expected one of: [%s]",
                                                clazz, Arrays.toString(clazz.getEnumConstants()))));
    }

    static Duration convertToDuration(Object o) {
        if (o == null) {
            return null;
        } else if (o.getClass() == Duration.class) {
            return (Duration) o;
        }

        return TimeUtils.parseDuration(o.toString());
    }

    static String convertToString(Object o) {
        if (o == null) {
            return null;
        } else if (o.getClass() == String.class) {
            return (String) o;
        } else if (o.getClass() == Duration.class) {
            Duration duration = (Duration) o;
            return TimeUtils.formatWithHighestUnit(duration);
        } else if (o instanceof List) {
            return ((List<?>) o)
                    .stream()
                            .map(
                                    e ->
                                            StructuredOptionsSplitter.escapeWithSingleQuote(
                                                    convertToString(e), ";"))
                            .collect(Collectors.joining(";"));
        } else if (o instanceof Map) {
            return ((Map<?, ?>) o)
                    .entrySet().stream()
                            .map(
                                    e -> {
                                        String escapedKey =
                                                StructuredOptionsSplitter.escapeWithSingleQuote(
                                                        e.getKey().toString(), ":");
                                        String escapedValue =
                                                StructuredOptionsSplitter.escapeWithSingleQuote(
                                                        e.getValue().toString(), ":");

                                        return StructuredOptionsSplitter.escapeWithSingleQuote(
                                                escapedKey + ":" + escapedValue, ",");
                                    })
                            .collect(Collectors.joining(","));
        }

        return o.toString();
    }

    static Integer convertToInt(Object o) {
        if (o == null) {
            return null;
        } else if (o.getClass() == Integer.class) {
            return (Integer) o;
        } else if (o.getClass() == Long.class) {
            long value = (Long) o;
            if (value <= Integer.MAX_VALUE && value >= Integer.MIN_VALUE) {
                return (int) value;
            } else {
                throw new IllegalArgumentException(
                        String.format(
                                "Configuration value %s overflows/underflows the integer type.",
                                value));
            }
        }

        return Integer.parseInt(o.toString());
    }

    static Long convertToLong(Object o) {
        if (o == null) {
            return null;
        } else if (o.getClass() == Long.class) {
            return (Long) o;
        } else if (o.getClass() == Integer.class) {
            return ((Integer) o).longValue();
        }

        return Long.parseLong(o.toString());
    }

    static Boolean convertToBoolean(Object o) {
        if (o == null) {
            return null;
        } else if (o.getClass() == Boolean.class) {
            return (Boolean) o;
        }

        switch (o.toString().toUpperCase()) {
            case "TRUE":
                return true;
            case "FALSE":
                return false;
            default:
                throw new IllegalArgumentException(
                        String.format(
                                "Unrecognized option for boolean: %s. Expected either true or false(case insensitive)",
                                o));
        }
    }

    static Float convertToFloat(Object o) {
        if (o == null) {
            return null;
        }
        if (o.getClass() == Float.class) {
            return (Float) o;
        } else if (o.getClass() == Double.class) {
            double value = ((Double) o);
            if (value == 0.0
                    || (value >= Float.MIN_VALUE && value <= Float.MAX_VALUE)
                    || (value >= -Float.MAX_VALUE && value <= -Float.MIN_VALUE)) {
                return (float) value;
            } else {
                throw new IllegalArgumentException(
                        String.format(
                                "Configuration value %s overflows/underflows the float type.",
                                value));
            }
        }

        return Float.parseFloat(o.toString());
    }

    static Double convertToDouble(Object o) {
        if (o == null) {
            return null;
        }
        if (o.getClass() == Double.class) {
            return (Double) o;
        } else if (o.getClass() == Float.class) {
            return ((Float) o).doubleValue();
        }

        return Double.parseDouble(o.toString());
    }

    // --------------------------------------------------------------------------------------------
    //  Prefix map handling
    // --------------------------------------------------------------------------------------------

    /**
     * Maps can be represented in two ways.
     *
     * <p>With constant key space:
     *
     * <pre>
     *     avro-confluent.properties = schema: 1, other-prop: 2
     * </pre>
     *
     * <p>Or with variable key space (i.e. prefix notation):
     *
     * <pre>
     *     avro-confluent.properties.schema = 1
     *     avro-confluent.properties.other-prop = 2
     * </pre>
     */
    public static boolean canBePrefixMap(ConfigOption<?> configOption) {
        return configOption.getClazz() == Map.class && !configOption.isList();
    }

    /** Filter condition for prefix map keys. */
    public static boolean filterPrefixMapKey(String key, String candidate) {
        final String prefixKey = key + ".";
        return candidate.startsWith(prefixKey);
    }

    static Map<String, String> convertToPropertiesPrefixed(
            Map<String, Object> confData, String key) {
        final String prefixKey = key + ".";
        return confData.keySet().stream()
                .filter(k -> k.startsWith(prefixKey))
                .collect(
                        Collectors.toMap(
                                k -> k.substring(prefixKey.length()),
                                k -> convertToString(confData.get(k))));
    }

    static boolean containsPrefixMap(Map<String, Object> confData, String key) {
        return confData.keySet().stream().anyMatch(candidate -> filterPrefixMapKey(key, candidate));
    }

    static boolean removePrefixMap(Map<String, Object> confData, String key) {
        final List<String> prefixKeys =
                confData.keySet().stream()
                        .filter(candidate -> filterPrefixMapKey(key, candidate))
                        .collect(Collectors.toList());
        prefixKeys.forEach(confData::remove);
        return !prefixKeys.isEmpty();
    }

    // Make sure that we cannot instantiate this class
    private ConfigurationUtils() {}
}
