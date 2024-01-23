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

package com.ververica.cdc.connectors.paimon.sink.v2;

import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage;

import org.apache.paimon.flink.sink.ChannelComputer;
import org.apache.paimon.flink.sink.MultiTableCommittable;

import java.util.Objects;

/**
 * {@link ChannelComputer} for {@link MultiTableCommittable}. Shuffle by database and table, make
 * sure that MultiTableCommittable from the same table will be sent to the same channel.
 */
public class MultiTableCommittableChannelComputer
        implements ChannelComputer<CommittableMessage<MultiTableCommittable>> {

    private static final long serialVersionUID = 1L;

    private transient int numChannels;

    @Override
    public void setup(int numChannels) {
        this.numChannels = numChannels;
    }

    @Override
    public int channel(CommittableMessage<MultiTableCommittable> committableMessage) {
        if (committableMessage instanceof CommittableWithLineage) {
            MultiTableCommittable multiTableCommittable =
                    ((CommittableWithLineage<MultiTableCommittable>) committableMessage)
                            .getCommittable();
            return Math.floorMod(
                    Objects.hash(
                            multiTableCommittable.getDatabase(), multiTableCommittable.getTable()),
                    numChannels);
        } else {
            // CommittableSummary is inaccurate after shuffling, need to be recreated.
            return Math.floorMod(Objects.hash(committableMessage), numChannels);
        }
    }
}
