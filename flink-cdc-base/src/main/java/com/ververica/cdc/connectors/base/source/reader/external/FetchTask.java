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

package com.ververica.cdc.connectors.base.source.reader.external;

import org.apache.flink.annotation.Experimental;

import com.ververica.cdc.connectors.base.source.meta.offset.Offset;
import com.ververica.cdc.connectors.base.source.meta.split.SourceSplitBase;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/** The task to fetching data of a Split. */
@Experimental
public interface FetchTask<Split> {

    /** Execute current task. */
    void execute(Context context) throws Exception;

    /** Returns current task is running or not. */
    boolean isRunning();

    /** Returns the split that the task used. */
    Split getSplit();

    /** Base context used in the execution of fetch task. */
    interface Context {
        void configure(SourceSplitBase sourceSplitBase);

        ChangeEventQueue<DataChangeEvent> getQueue();

        TableId getTableId(SourceRecord record);

        Tables.TableFilter getTableFilter();

        Offset getStreamOffset(SourceRecord record);

        boolean isDataChangeRecord(SourceRecord record);

        boolean isRecordBetween(SourceRecord record, Object[] splitStart, Object[] splitEnd);

        void rewriteOutputBuffer(Map<Struct, SourceRecord> outputBuffer, SourceRecord changeRecord);

        List<SourceRecord> formatMessageTimestamp(Collection<SourceRecord> snapshotRecords);
    }
}
