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

package io.debezium.connector.oracle.logminer;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class UnistrHelperTest {

    @Test
    void shouldConvertUnistrValues() {
        assertThat(UnistrHelper.convert("UNISTR('\\0412\\044B')")).isEqualTo("\u0412\u044B");
        assertThat(UnistrHelper.convert("UNISTR('\\0412\\044B')||UNISTR('\\0431\\0443')"))
                .isEqualTo("\u0412\u044B\u0431\u0443");
        assertThat(UnistrHelper.convert("UNISTR('\\0412\\044B')  ||  UNISTR('\\0431\\0443')"))
                .isEqualTo("\u0412\u044B\u0431\u0443");
    }

    @Test
    void shouldConvertUnistrValueWithConcatenationCharacterSequence() {
        assertThat(UnistrHelper.convert("UNISTR('\\4E2D\\56FD||\\6B66\\6C49')"))
                .isEqualTo("\u4E2D\u56FD||\u6B66\u6C49");
    }

    @Test
    void shouldConvertUnistrValueWithEmbeddedConcatenationAndAsciiCharacters() {
        assertThat(
                        UnistrHelper.convert(
                                "UNISTR('\\592A\\5E73\\6D0B\\53CC\\514D4000||"
                                        + "\\518D\\5236\\9020\\5DF2\\5F55\\5355||"
                                        + "C440100VEH26071668')"))
                .isEqualTo(
                        "\u592A\u5E73\u6D0B\u53CC\u514D4000||"
                                + "\u518D\u5236\u9020\u5DF2\u5F55\u5355||"
                                + "C440100VEH26071668");
    }

    @Test
    void shouldNotDuplicateOriginalUnistrValueWhenConcatenationSequenceIsInsideFunction() {
        String unistr =
                "UNISTR('\\592A\\5E73\\6D0B\\53CC\\514D4000||"
                        + "\\518D\\5236\\9020\\5DF2\\5F55\\5355||"
                        + "C440100VEH26071668')";

        assertThat(UnistrHelper.convert(unistr)).doesNotContain("UNISTR(");
    }
}
