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

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

/** Unit test for {@link BufferedSocketInputStream}. */
class BufferedSocketInputStreamTest {

    @Test
    void testReadFromBufferedSocketInputStream() throws Exception {
        BufferedSocketInputStream in =
                new BufferedSocketInputStream(
                        new ByteArrayInputStream(
                                new byte[] {'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H'}));
        byte[] buf = new byte[3];
        Assertions.assertThat(in.read(buf, 0, buf.length)).isEqualTo(3);
        Assertions.assertThat(buf).isEqualTo(new byte[] {'A', 'B', 'C'});
        Assertions.assertThat(in.available()).isEqualTo(5);

        Assertions.assertThat(in.read(buf, 0, buf.length)).isEqualTo(3);
        Assertions.assertThat(buf).isEqualTo(new byte[] {'D', 'E', 'F'});
        Assertions.assertThat(in.available()).isEqualTo(2);

        buf = new byte[2];
        Assertions.assertThat(in.read(buf, 0, buf.length)).isEqualTo(2);
        Assertions.assertThat(buf).isEqualTo(new byte[] {'G', 'H'});
        Assertions.assertThat(in.available()).isZero();

        // reach the end of stream normally
        Assertions.assertThat(in.read(buf, 0, buf.length)).isEqualTo(-1);
        Assertions.assertThat(in.available()).isZero();
    }
}
