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

package com.ververica.cdc.connectors.values.source;

import com.ververica.cdc.common.configuration.ConfigOption;
import com.ververica.cdc.common.configuration.ConfigOptions;
import com.ververica.cdc.common.configuration.description.Description;
import com.ververica.cdc.common.configuration.description.ListElement;

import static com.ververica.cdc.common.configuration.description.TextElement.text;

/** Configurations for {@link ValuesDataSource}. */
public class ValuesDataSourceOptions {

    public static final ConfigOption<ValuesDataSourceHelper.SourceEventType> SOURCE_EVENT_TYPE =
            ConfigOptions.key("source.event.type")
                    .enumType(ValuesDataSourceHelper.SourceEventType.class)
                    .defaultValue(ValuesDataSourceHelper.SourceEventType.SINGLE_SPLIT_SINGLE_TABLE)
                    .withDescription(
                            Description.builder()
                                    .text("Type of creating source change events. ")
                                    .linebreak()
                                    .add(
                                            ListElement.list(
                                                    text(
                                                            "SINGLE_SPLIT_SINGLE_TABLE: Default and predetermined case. Creating schema changes of single table and put them into one split."),
                                                    text(
                                                            "SINGLE_SPLIT_MULTI_TABLES: A predetermined case. Creating schema changes of multiple tables and put them into one split."),
                                                    text(
                                                            "CUSTOM_SOURCE_EVENTS: Passed change events by the user through calling `setSourceEvents` method.")))
                                    .build());

    public static final ConfigOption<Integer> FAILURE_INJECTION_INDEX =
            ConfigOptions.key("failure.injection.index")
                    .intType()
                    .defaultValue(Integer.MAX_VALUE)
                    .withDescription(
                            "Specific index of test events to fail, set a Integer.MAX_VALUE value by default to avoid failure.");
}
