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

package org.apache.flink.cdc.connectors.mysql.source.utils;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit test for {@link org.apache.flink.cdc.connectors.mysql.source.utils.StatementUtils}. */
class StatementUtilsTest {

    @Test
    void testSetSafeObjectCorrectlyHandlesOverflow() throws SQLException {
        Map<String, Object> invocationDetails = new HashMap<>();
        PreparedStatement psProxy = createPreparedStatementProxy(invocationDetails);

        long overflowValue = Long.MAX_VALUE + 1L;
        BigDecimal expectedBigDecimal = new BigDecimal(Long.toUnsignedString(overflowValue));

        // Use the safe method
        StatementUtils.setSafeObject(psProxy, 1, overflowValue);

        // Assert that it correctly used setBigDecimal with the converted value
        assertThat(invocationDetails.get("methodName")).isEqualTo("setBigDecimal");
        assertThat(invocationDetails.get("value")).isEqualTo(expectedBigDecimal);
    }

    @Test
    void testDirectSetObjectFailsOnOverflow() throws SQLException {
        Map<String, Object> invocationDetails = new HashMap<>();
        PreparedStatement psProxy = createPreparedStatementProxy(invocationDetails);

        long overflowValue = Long.MAX_VALUE + 1L;

        // Directly call the unsafe method on the proxy
        psProxy.setObject(1, overflowValue);

        // Assert that it incorrectly used setObject, preserving the wrong negative long value
        assertThat(invocationDetails.get("methodName")).isEqualTo("setObject");
        assertThat(invocationDetails.get("value")).isInstanceOf(Long.class);
        assertThat(invocationDetails.get("value")).isEqualTo(Long.MIN_VALUE);
    }

    @Test
    void testSetSafeObjectHandlesRegularValues() throws SQLException {
        Map<String, Object> invocationDetails = new HashMap<>();
        PreparedStatement psProxy = createPreparedStatementProxy(invocationDetails);

        // Test with a common Long
        StatementUtils.setSafeObject(psProxy, 1, 123L);
        assertThat(invocationDetails.get("methodName")).isEqualTo("setObject");
        assertThat(invocationDetails.get("value")).isEqualTo(123L);
        invocationDetails.clear();

        // Test with a String
        StatementUtils.setSafeObject(psProxy, 2, "test");
        assertThat(invocationDetails.get("methodName")).isEqualTo("setObject");
        assertThat(invocationDetails.get("value")).isEqualTo("test");
        invocationDetails.clear();

        // Test with null
        StatementUtils.setSafeObject(psProxy, 3, null);
        assertThat(invocationDetails.get("methodName")).isEqualTo("setObject");
        assertThat(invocationDetails.get("value")).isNull();
        invocationDetails.clear();
    }

    private PreparedStatement createPreparedStatementProxy(Map<String, Object> invocationDetails) {
        return (PreparedStatement)
                Proxy.newProxyInstance(
                        StatementUtilsTest.class.getClassLoader(),
                        new Class<?>[] {PreparedStatement.class},
                        (proxy, method, args) -> {
                            String methodName = method.getName();
                            if (methodName.equals("setObject")
                                    || methodName.equals("setBigDecimal")) {
                                invocationDetails.put("methodName", methodName);
                                invocationDetails.put("parameterIndex", args[0]);
                                invocationDetails.put("value", args[1]);
                            }
                            return null;
                        });
    }
}
