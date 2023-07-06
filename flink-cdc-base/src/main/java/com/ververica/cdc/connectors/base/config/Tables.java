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

package com.ververica.cdc.connectors.base.config;

import org.apache.flink.annotation.Experimental;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

/** A list of tables to include or exclude from extraction. */
@Experimental
public class Tables implements Serializable {

    private final TableOption tableType;
    private final List<String> tableList;

    private Tables(TableOption tableType, List<String> tableList) {
        this.tableType = tableType;
        this.tableList = tableList;
    }

    public boolean isToInclude() {
        return TableOption.INCLUDE.equals(this.tableType);
    }

    public boolean isToExclude() {
        return TableOption.EXCLUDE.equals(this.tableType);
    }

    public List<String> getTableList() {
        return this.tableList;
    }

    public static Tables include(String... tableList) {
        return include(Arrays.asList(tableList));
    }

    public static Tables include(List<String> tableList) {
        return new Tables(TableOption.INCLUDE, tableList);
    }

    public static Tables exclude(String... tableList) {
        return exclude(Arrays.asList(tableList));
    }

    public static Tables exclude(List<String> tableList) {
        return new Tables(TableOption.EXCLUDE, tableList);
    }

    /** Type for tables. */
    public enum TableOption implements Serializable {
        EXCLUDE,
        INCLUDE
    }
}
