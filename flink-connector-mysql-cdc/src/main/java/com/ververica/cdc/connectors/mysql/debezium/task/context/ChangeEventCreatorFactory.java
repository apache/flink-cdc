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

package com.ververica.cdc.connectors.mysql.debezium.task.context;

import com.ververica.cdc.connectors.mysql.source.offset.BinlogOffsetKind;
import com.ververica.cdc.connectors.mysql.source.split.MySqlSplit;
import com.ververica.cdc.connectors.mysql.source.utils.RecordUtils;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.spi.ChangeEventCreator;

/** Factory class for creating {@link ChangeEventCreator}. */
public class ChangeEventCreatorFactory {

    /**
     * Create a {@link ChangeEventCreator} according to the assigned split.
     *
     * <p>If a binlog split is assigned, and the starting binlog offset has {@link
     * BinlogOffsetKind#TIMESTAMP} kind, we need to apply a filter onto the creator, which drops
     * events earlier than the specified timestamp.
     *
     * @param split the assigned split
     */
    public static ChangeEventCreator createChangeEventCreator(MySqlSplit split) {
        if (split.isBinlogSplit()
                && split.asBinlogSplit()
                        .getStartingOffset()
                        .getOffsetKind()
                        .equals(BinlogOffsetKind.TIMESTAMP)) {
            long startTimestampSec = split.asBinlogSplit().getStartingOffset().getTimestampSec();
            return sourceRecord -> {
                Long messageTimestampMs = RecordUtils.getMessageTimestamp(sourceRecord);
                if (messageTimestampMs != null && messageTimestampMs / 1000 >= startTimestampSec) {
                    return new DataChangeEvent(sourceRecord);
                } else {
                    return null;
                }
            };
        }

        return DataChangeEvent::new;
    }
}
