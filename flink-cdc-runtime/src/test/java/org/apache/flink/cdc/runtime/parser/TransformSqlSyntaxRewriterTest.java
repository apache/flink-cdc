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

package org.apache.flink.cdc.runtime.parser;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

class TransformSqlSyntaxRewriterTest {

    @Test
    void rewritesTryCastSyntax() {
        Assertions.assertThat(
                        TransformSqlSyntaxRewriter.rewriteTryCast(
                                "SELECT TRY_CAST(value AS INT) FROM t"))
                .isEqualTo("SELECT TRY_CAST(CAST(value AS INT)) FROM t");
        Assertions.assertThat(
                        TransformSqlSyntaxRewriter.rewriteTryCast(
                                "SELECT try_cast /* marker */ (value AS DECIMAL(10, 2)) FROM t"))
                .isEqualTo("SELECT try_cast /* marker */ (CAST(value AS DECIMAL(10, 2))) FROM t");
        Assertions.assertThat(
                        TransformSqlSyntaxRewriter.rewriteTryCast(
                                "SELECT TrY_CaSt(COALESCE(a, CAST(b AS INT)) AS VARCHAR) FROM t"))
                .isEqualTo("SELECT TrY_CaSt(CAST(COALESCE(a, CAST(b AS INT)) AS VARCHAR)) FROM t");
    }

    @Test
    void rewritesNestedTryCastSyntax() {
        Assertions.assertThat(
                        TransformSqlSyntaxRewriter.rewriteTryCast(
                                "TRY_CAST(TRY_CAST(value AS INT) AS VARCHAR)"))
                .isEqualTo("TRY_CAST(CAST(TRY_CAST(CAST(value AS INT)) AS VARCHAR))");
    }

    @Test
    void ignoresTryCastInProtectedRegionsAndSimilarIdentifiers() {
        String sql =
                "SELECT 'TRY_CAST(a AS INT)', \"TRY_CAST(b AS INT)\", "
                        + "`TRY_CAST(c AS INT)`, MY_TRY_CAST(d AS INT), TRY_CASTED "
                        + "-- TRY_CAST(e AS INT)\n"
                        + "/* TRY_CAST(f AS INT) */ FROM t";
        Assertions.assertThat(TransformSqlSyntaxRewriter.rewriteTryCast(sql)).isEqualTo(sql);
    }

    @Test
    void preservesQuotedEscapesAndCommentsInsideTryCast() {
        String sql =
                "TRY_CAST(CASE WHEN value = 'it''s TRY_CAST(x AS INT)' "
                        + "THEN /* AS VARCHAR */ \"quoted\" ELSE `AS` END AS VARCHAR)";
        Assertions.assertThat(TransformSqlSyntaxRewriter.rewriteTryCast(sql))
                .isEqualTo(
                        "TRY_CAST(CAST(CASE WHEN value = 'it''s TRY_CAST(x AS INT)' "
                                + "THEN /* AS VARCHAR */ \"quoted\" ELSE `AS` END AS VARCHAR))");
    }

    @Test
    void leavesMalformedTryCastForCalciteToReject() {
        Assertions.assertThat(TransformSqlSyntaxRewriter.rewriteTryCast("TRY_CAST(value)"))
                .isEqualTo("TRY_CAST(value)");
        Assertions.assertThat(TransformSqlSyntaxRewriter.rewriteTryCast("TRY_CAST(value AS INT"))
                .isEqualTo("TRY_CAST(value AS INT");
    }
}
