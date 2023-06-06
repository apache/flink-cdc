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

package com.ververica.cdc.connectors.sqlserver.source.offset;

import com.ververica.cdc.connectors.base.source.meta.offset.Offset;
import com.ververica.cdc.connectors.base.source.meta.offset.OffsetFactory;
import io.debezium.connector.sqlserver.Lsn;
import io.debezium.connector.sqlserver.SourceInfo;

import java.util.Map;

/** A factory to create {@link LsnOffset}. */
public class LsnFactory extends OffsetFactory {
    @Override
    public Offset newOffset(Map<String, String> offset) {
        Lsn changeLsn = Lsn.valueOf(offset.get(SourceInfo.CHANGE_LSN_KEY));
        Lsn commitLsn = Lsn.valueOf(offset.get(SourceInfo.COMMIT_LSN_KEY));
        return new LsnOffset(changeLsn, commitLsn, null);
    }

    @Override
    public Offset newOffset(String filename, Long position) {
        throw new UnsupportedOperationException(
                "not supported create new Offset by filename and position.");
    }

    @Override
    public Offset newOffset(Long position) {
        throw new UnsupportedOperationException(
                "not supported create new Offset by filename and position.");
    }

    @Override
    public Offset createInitialOffset() {
        return LsnOffset.INITIAL_OFFSET;
    }

    @Override
    public Offset createNoStoppingOffset() {
        return LsnOffset.NO_STOPPING_OFFSET;
    }
}
