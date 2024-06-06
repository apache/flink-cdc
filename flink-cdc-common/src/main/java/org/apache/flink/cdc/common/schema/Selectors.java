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

package org.apache.flink.cdc.common.schema;

import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.utils.Predicates;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

/** Selectors for filtering tables. */
public class Selectors {

    private List<Selector> selectors;

    private Selectors() {}

    /**
     * A {@link Selector} that determines whether a table identified by a given {@link TableId} is
     * to be included.
     */
    private static class Selector {
        private final Predicate<String> namespacePred;
        private final Predicate<String> schemaNamePred;
        private final Predicate<String> tableNamePred;

        public Selector(String namespace, String schemaName, String tableName) {
            this.namespacePred =
                    namespace == null ? (namespacePred) -> false : Predicates.includes(namespace);
            this.schemaNamePred =
                    schemaName == null
                            ? (schemaNamePred) -> false
                            : Predicates.includes(schemaName);
            this.tableNamePred =
                    tableName == null ? (tableNamePred) -> false : Predicates.includes(tableName);
        }

        public boolean isMatch(TableId tableId) {

            String namespace = tableId.getNamespace();
            String schemaName = tableId.getSchemaName();

            if (namespace == null || namespace.isEmpty()) {
                if (schemaName == null || schemaName.isEmpty()) {
                    return tableNamePred.test(tableId.getTableName());
                }
                return schemaNamePred.test(tableId.getSchemaName())
                        && tableNamePred.test(tableId.getTableName());
            }
            return namespacePred.test(tableId.getNamespace())
                    && schemaNamePred.test(tableId.getSchemaName())
                    && tableNamePred.test(tableId.getTableName());
        }
    }

    /** Match the {@link TableId} against the {@link Selector}s. * */
    public boolean isMatch(TableId tableId) {
        for (Selector selector : selectors) {
            if (selector.isMatch(tableId)) {
                return true;
            }
        }
        return false;
    }

    /** Builder for {@link Selectors}. */
    public static class SelectorsBuilder {

        private List<Selector> selectors;

        public SelectorsBuilder includeTables(String tableInclusions) {

            if (tableInclusions == null || tableInclusions.isEmpty()) {
                throw new IllegalArgumentException(
                        "Invalid table inclusion pattern cannot be null or empty");
            }

            List<Selector> selectors = new ArrayList<>();
            Set<String> tableSplitSet =
                    Predicates.setOf(
                            tableInclusions, Predicates.RegExSplitterByComma::split, (str) -> str);
            for (String tableSplit : tableSplitSet) {
                List<String> tableIdList =
                        Predicates.listOf(
                                tableSplit, Predicates.RegExSplitterByDot::split, (str) -> str);
                Iterator<String> iterator = tableIdList.iterator();
                if (tableIdList.size() == 1) {
                    selectors.add(new Selector(null, null, iterator.next()));
                } else if (tableIdList.size() == 2) {
                    selectors.add(new Selector(null, iterator.next(), iterator.next()));
                } else if (tableIdList.size() == 3) {
                    selectors.add(new Selector(iterator.next(), iterator.next(), iterator.next()));
                } else {
                    throw new IllegalArgumentException(
                            "Invalid table inclusion pattern: " + tableInclusions);
                }
            }
            this.selectors = selectors;
            return this;
        }

        public Selectors build() {
            Selectors selectors = new Selectors();
            selectors.selectors = this.selectors;
            return selectors;
        }
    }
}
