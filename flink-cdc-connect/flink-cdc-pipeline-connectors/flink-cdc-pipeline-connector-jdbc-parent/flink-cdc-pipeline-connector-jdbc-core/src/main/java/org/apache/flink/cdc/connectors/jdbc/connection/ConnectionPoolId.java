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

package org.apache.flink.cdc.connectors.jdbc.connection;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Objects;

/** A unique identifier of connection pool. */
public class ConnectionPoolId implements Serializable {
    private static final long serialVersionUID = 1L;
    private final String host;
    private final int port;
    private final String username;

    private final String dataSourcePoolFactoryIdentifier;

    public ConnectionPoolId(
            String host,
            int port,
            String username,
            @Nullable String database,
            String dataSourcePoolFactoryIdentifier) {
        this.host = host;
        this.port = port;
        this.username = username;
        this.dataSourcePoolFactoryIdentifier = dataSourcePoolFactoryIdentifier;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ConnectionPoolId)) {
            return false;
        }
        ConnectionPoolId that = (ConnectionPoolId) o;
        return Objects.equals(host, that.host)
                && Objects.equals(port, that.port)
                && Objects.equals(username, that.username)
                && Objects.equals(
                        dataSourcePoolFactoryIdentifier, that.dataSourcePoolFactoryIdentifier);
    }

    @Override
    public int hashCode() {
        return Objects.hash(host, port, username, dataSourcePoolFactoryIdentifier);
    }

    @Override
    public String toString() {
        return username
                + '@'
                + host
                + ':'
                + port
                + ", dataSourcePoolFactoryIdentifier="
                + dataSourcePoolFactoryIdentifier;
    }

    public String getDataSourcePoolFactoryIdentifier() {
        return dataSourcePoolFactoryIdentifier;
    }
}
