/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.tdsql.source.events;

import org.apache.flink.api.connector.source.SourceEvent;

import com.ververica.cdc.connectors.tdsql.bases.set.TdSqlSet;

/** send mysql event with tdsql set info. */
public class TdSqlSourceEvent implements SourceEvent {
    private static final long serialVersionUID = -5194600926649657532L;

    private final SourceEvent mySqlEvent;
    private final TdSqlSet set;

    public TdSqlSourceEvent(SourceEvent mySqlEvent, TdSqlSet set) {
        this.mySqlEvent = mySqlEvent;
        this.set = set;
    }

    public SourceEvent getMySqlEvent() {
        return mySqlEvent;
    }

    public TdSqlSet getSet() {
        return set;
    }

    @Override
    public String toString() {
        return "TdSqlSourceEvent{" + "mySqlEvent=" + mySqlEvent + ", set=" + set + '}';
    }
}
