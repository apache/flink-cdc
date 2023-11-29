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

    public static final ConfigOption<ValuesDataSourceHelper.EventSetId> EVENT_SET_ID =
            ConfigOptions.key("event-set.id")
                    .enumType(ValuesDataSourceHelper.EventSetId.class)
                    .defaultValue(ValuesDataSourceHelper.EventSetId.SINGLE_SPLIT_SINGLE_TABLE)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Id for creating source change events from ValuesDataSourceHelper.EventSetId.")
                                    .linebreak()
                                    .add(
                                            ListElement.list(
                                                    text(
                                                            "SINGLE_SPLIT_SINGLE_TABLE: Default and predetermined case. Creating schema changes of single table and put them into one split."),
                                                    text(
                                                            "SINGLE_SPLIT_MULTI_TABLES: A predetermined case. Creating schema changes of multiple tables and put them into one split."),
                                                    text(
                                                            "MULTI_SPLITS_SINGLE_TABLE: A predetermined case. Creating schema changes of single table and put them into multiple splits."),
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
