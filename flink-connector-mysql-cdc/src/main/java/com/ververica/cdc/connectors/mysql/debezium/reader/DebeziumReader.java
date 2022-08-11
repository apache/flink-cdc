/*
 * Copyright 2022 Ververica Inc.
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

package com.ververica.cdc.connectors.mysql.debezium.reader;

import javax.annotation.Nullable;

import java.util.Iterator;

/** Reader to read split of table, the split is either snapshot split or binlog split. */
public interface DebeziumReader<T, Split> {

    /** Return the current split of the reader is finished or not. */
    boolean isFinished();

    /**
     * Add to split to read, this should call only the when reader is idle.
     *
     * @param splitToRead
     */
    void submitSplit(Split splitToRead);

    /** Close the reader and releases all resources. */
    void close();

    /**
     * Reads records from MySQL. The method should return null when reaching the end of the split,
     * the empty {@link Iterator} will be returned if the data of split is on pulling.
     */
    @Nullable
    Iterator<T> pollSplitRecords() throws InterruptedException;
}
