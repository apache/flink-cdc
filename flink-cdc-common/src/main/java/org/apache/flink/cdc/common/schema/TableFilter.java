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

import java.util.function.Predicate;

/** A filter for tables. */
@FunctionalInterface
public interface TableFilter {

    /** Determines whether the given table should be included. */
    boolean isIncluded(TableId tableId);

    /** Creates a {@link TableFilter} from the given predicate. */
    static TableFilter fromPredicate(Predicate<TableId> predicate) {
        return predicate::test;
    }

    /** Creates a {@link TableFilter} that includes all tables. */
    static TableFilter includeAll() {
        return t -> true;
    }
}
