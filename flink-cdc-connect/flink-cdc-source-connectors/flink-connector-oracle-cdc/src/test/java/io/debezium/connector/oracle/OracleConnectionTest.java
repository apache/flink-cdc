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

package io.debezium.connector.oracle;

import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for Oracle SCN timestamp compatibility helpers. */
class OracleConnectionTest {

    @Test
    void testReadScnTimestampUsesTimestampMapping() throws Exception {
        Timestamp timestamp = Timestamp.valueOf("2026-04-08 13:49:08");
        ResultSet rs = resultSet(timestamp);

        Instant actual = OracleConnection.readScnTimestamp(rs, 1);

        assertThat(actual).isEqualTo(timestamp.toInstant());
    }

    @Test
    void testReadScnTimestampAllowsNullTimestamp() throws Exception {
        ResultSet rs = resultSet(null);

        Instant actual = OracleConnection.readScnTimestamp(rs, 1);

        assertThat(actual).isNull();
    }

    private static ResultSet resultSet(Timestamp timestamp) {
        InvocationHandler handler =
                (proxy, method, args) -> {
                    String name = method.getName();
                    if ("getTimestamp".equals(name)) {
                        return timestamp;
                    }
                    if ("getObject".equals(name) && args != null && args.length == 2) {
                        throw new SQLException("OffsetDateTime path should not be used");
                    }
                    if ("wasNull".equals(name)) {
                        return timestamp == null;
                    }
                    if ("unwrap".equals(name)) {
                        return null;
                    }
                    if ("isWrapperFor".equals(name)) {
                        return false;
                    }
                    throw new UnsupportedOperationException("Unexpected method: " + name);
                };
        return (ResultSet)
                Proxy.newProxyInstance(
                        OracleConnectionTest.class.getClassLoader(),
                        new Class[] {ResultSet.class},
                        handler);
    }
}
