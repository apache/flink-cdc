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

package com.ververica.cdc.composer.definition;

import javax.annotation.Nullable;

import java.util.Objects;
import java.util.Optional;

/**
 * Definition of a router.
 *
 * <p>A mapper definition contains:
 *
 * <ul>
 *   <li>sourceDataBase: sourceDataBase.
 *   <li>sinkDataBase: sinkDataBase.
 *   <li>tablePrefix: tablePrefix.
 *   <li>tableSuffix: tableSuffix.
 * </ul>
 */
public class SchemaRouteDef {
    private final String sourceDatabase;
    private final String sinkDatabase;
    @Nullable private final String tablePrefix;
    @Nullable private final String tableSuffix;

    public SchemaRouteDef(
            String sourceDatabase,
            String sinkDatabase,
            @Nullable String tablePrefix,
            @Nullable String tableSuffix) {
        this.sourceDatabase = sourceDatabase;
        this.sinkDatabase = sinkDatabase;
        this.tablePrefix = tablePrefix;
        this.tableSuffix = tableSuffix;
    }

    public String getSourceDatabase() {
        return sourceDatabase;
    }

    public String getSinkDatabase() {
        return sinkDatabase;
    }

    public Optional<String> getTablePrefix() {
        return Optional.ofNullable(tablePrefix);
    }

    public Optional<String> getTableSuffix() {
        return Optional.ofNullable(tableSuffix);
    }

    @Override
    public String toString() {
        return "MapperDef{"
                + "sourceDatabase='"
                + sourceDatabase
                + '\''
                + ", sinkDatabase='"
                + sinkDatabase
                + '\''
                + ", tablePrefix='"
                + tablePrefix
                + '\''
                + ", tableSuffix='"
                + tableSuffix
                + '\''
                + '}';
    }

    @Override
    public int hashCode() {
        return Objects.hash(sourceDatabase, sinkDatabase, tablePrefix, tableSuffix);
    }
}
