package com.alibaba.ververica.cdc.debezium.table;

import java.util.Map;
import java.util.Properties;

/** Option utils for Debezium options. */
public class DebeziumOptions {
	public static final String DEBEZIUM_OPTIONS_PREFIX = "debezium.";

	public static Properties getDebeziumProperties(Map<String, String> properties) {
		final Properties debeziumProperties = new Properties();

		if (hasDebeziumProperties(properties)) {
			properties.keySet().stream()
				.filter(key -> key.startsWith(DEBEZIUM_OPTIONS_PREFIX))
				.forEach(key -> {
					final String value = properties.get(key);
					final String subKey = key.substring((DEBEZIUM_OPTIONS_PREFIX).length());
					debeziumProperties.put(subKey, value);
				});
		}
		return debeziumProperties;
	}

	/** Decides if the table options contains Debezium client properties that start with prefix 'debezium'. */
	private static boolean hasDebeziumProperties(Map<String, String> debeziumOptions) {
		return debeziumOptions.keySet().stream().anyMatch(k -> k.startsWith(DEBEZIUM_OPTIONS_PREFIX));
	}
}
