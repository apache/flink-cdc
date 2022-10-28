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

package com.ververica.cdc.connectors.tdsql.bases.set;

import java.io.Serializable;
import java.util.Objects;

/** tdsql set info. */
public class TdSqlSet implements Serializable {
    private static final long serialVersionUID = -3395810101971522360L;
    private String setKey;

    private String host;

    private int port;

    public TdSqlSet() {}

    public TdSqlSet(String setId, String host, int port) {
        this.setKey = setId;
        this.host = host;
        this.port = port;
    }

    public String getSetKey() {
        return setKey;
    }

    public void setSetKey(String setKey) {
        this.setKey = setKey;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof TdSqlSet)) {
            return false;
        }
        TdSqlSet tdSqlSet = (TdSqlSet) o;
        return port == tdSqlSet.port
                && setKey.equals(tdSqlSet.setKey)
                && host.equals(tdSqlSet.host);
    }

    @Override
    public int hashCode() {
        return Objects.hash(setKey, host, port);
    }

    @Override
    public String toString() {
        return "TdSqlSet{"
                + "setKey='"
                + setKey
                + '\''
                + ", host='"
                + host
                + '\''
                + ", port="
                + port
                + '}';
    }
}
