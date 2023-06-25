/*
 * Copyright 2023 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.db2.utils;

import org.apache.flink.configuration.ConfigurationUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/** A utility class to print configuration of connectors. */
public class OptionUtils {

    private static final Logger LOG = LoggerFactory.getLogger(OptionUtils.class);

    /** Utility class can not be instantiated. */
    private OptionUtils() {}

    public static void printOptions(String identifier, Map<String, String> config) {
        Map<String, String> hideMap = ConfigurationUtils.hideSensitiveValues(config);
        LOG.info("Print {} connector configuration:", identifier);
        for (String key : hideMap.keySet()) {
            LOG.info("{} = {}", key, hideMap.get(key));
        }
    }
}
