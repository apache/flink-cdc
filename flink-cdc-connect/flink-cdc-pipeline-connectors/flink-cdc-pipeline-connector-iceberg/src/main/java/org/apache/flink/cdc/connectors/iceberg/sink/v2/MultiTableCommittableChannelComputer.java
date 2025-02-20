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

package org.apache.flink.cdc.connectors.iceberg.sink.v2;

import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage;

import java.io.Serializable;
import java.util.Objects;

/**
 * Shuffle by tableId make it same hash in same slot
 */
public class MultiTableCommittableChannelComputer implements Serializable {

    private static final long serialVersionUID = 1L;

    private transient int numChannels;

    public void setup(int numChannels) {
        this.numChannels = numChannels;
    }

    public int channel(CommittableMessage<WriteResultWrapper> committableMessage) {
        if (committableMessage instanceof CommittableWithLineage) {
            WriteResultWrapper multiTableCommittable =
                    ((CommittableWithLineage<WriteResultWrapper>) committableMessage)
                            .getCommittable();
            TableId tableId = multiTableCommittable.getTableId();
            return Math.floorMod(Objects.hash(tableId.toString()), numChannels);
        } else {
            // CommittableSummary is inaccurate after shuffling, need to be recreated.
            return Math.floorMod(Objects.hash(committableMessage), numChannels);
        }
    }

    @Override
    public String toString() {
        return "shuffle by iceberg table";
    }
}
