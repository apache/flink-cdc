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

package com.github.shyiko.mysql.binlog.io;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit test for {@link BufferedSocketInputStream}. */
class BufferedSocketInputStreamTest {

    @Test
    void testReadFromBufferedSocketInputStream() throws Exception {
        BufferedSocketInputStream in =
                new BufferedSocketInputStream(
                        new ByteArrayInputStream(
                                new byte[] {'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H'}));
        byte[] buf = new byte[3];
        assertThat(in.read(buf, 0, buf.length)).isEqualTo(3);
        assertThat(buf).isEqualTo(new byte[] {'A', 'B', 'C'});
        assertThat(in.available()).isEqualTo(5);

        assertThat(in.read(buf, 0, buf.length)).isEqualTo(3);
        assertThat(buf).isEqualTo(new byte[] {'D', 'E', 'F'});
        assertThat(in.available()).isEqualTo(2);

        buf = new byte[2];
        assertThat(in.read(buf, 0, buf.length)).isEqualTo(2);
        assertThat(buf).isEqualTo(new byte[] {'G', 'H'});
        assertThat(in.available()).isZero();

        // reach the end of stream normally
        assertThat(in.read(buf, 0, buf.length)).isEqualTo(-1);
        assertThat(in.available()).isZero();
    }
}
