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

package com.ververica.cdc.connectors.tdsql.source.split;

import com.ververica.cdc.connectors.mysql.source.split.MySqlSplit;
import com.ververica.cdc.connectors.mysql.source.split.MySqlSplitState;
import com.ververica.cdc.connectors.tdsql.bases.set.TdSqlSet;

/**
 * State of the reader, essentially a mutable version of the {@link
 * com.ververica.cdc.connectors.tdsql.source.assigner.splitter.TdSqlSplit}.
 */
public class TdSqlSplitState {
    private final TdSqlSet setInfo;

    private final MySqlSplitState mySqlSplitState;

    public TdSqlSplitState(TdSqlSet setInfo, MySqlSplitState mySqlSplitState) {
        this.setInfo = setInfo;
        this.mySqlSplitState = mySqlSplitState;
    }

    public TdSqlSet setInfo() {
        return setInfo;
    }

    public MySqlSplitState mySqlSplitState() {
        return mySqlSplitState;
    }

    public MySqlSplit mySqlSplit() {
        return mySqlSplitState.toMySqlSplit();
    }
}
