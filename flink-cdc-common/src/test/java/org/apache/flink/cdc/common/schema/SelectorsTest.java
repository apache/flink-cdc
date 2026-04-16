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

package org.apache.flink.cdc.common.schema;

import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.pipeline.PipelineOptions;
import org.apache.flink.cdc.common.utils.Predicates;

import org.junit.jupiter.api.Test;

import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link org.apache.flink.cdc.common.schema.Selectors}. */
class SelectorsTest {

    @Test
    void testTableSelector() {

        // nameSpace, schemaName, tableName
        Selectors selectors =
                new Selectors.SelectorsBuilder()
                        .includeTables("db.sc1.A[0-9]+,db.sc2.B[0-1]+,db.sc1.sc1")
                        .build();

        assertAllowed(selectors, "db", "sc1", "sc1");
        assertAllowed(selectors, "db", "sc1", "A1");
        assertAllowed(selectors, "db", "sc1", "A2");
        assertAllowed(selectors, "db", "sc2", "B0");
        assertAllowed(selectors, "db", "sc2", "B1");
        assertNotAllowed(selectors, "db", "sc1", "A");
        assertNotAllowed(selectors, "db", "sc1a", "B");
        assertNotAllowed(selectors, "db", "sc1", "AA");
        assertNotAllowed(selectors, "db", "sc2", "B2");
        assertNotAllowed(selectors, "db2", "sc1", "A1");
        assertNotAllowed(selectors, "db2", "sc1", "A2");
        assertNotAllowed(selectors, "db", "sc11", "A1");
        assertNotAllowed(selectors, "db", "sc1A", "A1");

        selectors =
                new Selectors.SelectorsBuilder()
                        .includeTables("db\\..sc1.A[0-9]+,db.sc2.B[0-1]+,db\\..sc1.sc1,db.sc1.sc1")
                        .build();

        assertAllowed(selectors, "db", "sc1", "sc1");
        assertAllowed(selectors, "db1", "sc1", "sc1");
        assertAllowed(selectors, "dba", "sc1", "sc1");
        assertAllowed(selectors, "db1", "sc1", "A1");
        assertAllowed(selectors, "dba", "sc1", "A2");
        assertAllowed(selectors, "db", "sc2", "B0");
        assertAllowed(selectors, "db", "sc2", "B1");
        assertNotAllowed(selectors, "db", "sc1", "A");
        assertNotAllowed(selectors, "db", "sc1a", "B");
        assertNotAllowed(selectors, "db", "sc1", "AA");
        assertNotAllowed(selectors, "db", "sc2", "B2");
        assertNotAllowed(selectors, "dba1", "sc1", "A1");
        assertNotAllowed(selectors, "dba2", "sc1", "A2");
        assertNotAllowed(selectors, "db", "sc11", "A1");
        assertNotAllowed(selectors, "db", "sc1A", "A1");

        // schemaName, tableName
        selectors =
                new Selectors.SelectorsBuilder()
                        .includeTables("sc1.A[0-9]+,sc2.B[0-1]+,sc1.sc1")
                        .build();

        assertAllowed(selectors, null, "sc1", "sc1");
        assertAllowed(selectors, null, "sc1", "A1");
        assertAllowed(selectors, null, "sc1", "A2");
        assertAllowed(selectors, null, "sc2", "B0");
        assertAllowed(selectors, null, "sc2", "B1");
        assertNotAllowed(selectors, "db", "sc1", "A1");
        assertNotAllowed(selectors, null, "sc1", "A");
        assertNotAllowed(selectors, null, "sc2", "B");
        assertNotAllowed(selectors, null, "sc1", "AA");
        assertNotAllowed(selectors, null, "sc11", "A1");
        assertNotAllowed(selectors, null, "sc1A", "A1");

        // tableName
        selectors =
                new Selectors.SelectorsBuilder().includeTables("\\.A[0-9]+,B[0-1]+,sc1").build();

        assertAllowed(selectors, null, null, "sc1");
        assertNotAllowed(selectors, "db", "sc1", "sc1");
        assertNotAllowed(selectors, null, "sc1", "sc1");
        assertAllowed(selectors, null, null, "1A1");
        assertAllowed(selectors, null, null, "AA2");
        assertAllowed(selectors, null, null, "B0");
        assertAllowed(selectors, null, null, "B1");
        assertNotAllowed(selectors, "db", "sc1", "A1");
        assertNotAllowed(selectors, null, null, "A");
        assertNotAllowed(selectors, null, null, "B");
        assertNotAllowed(selectors, null, null, "2B");

        selectors =
                new Selectors.SelectorsBuilder()
                        .includeTables("sc1.A[0-9]+,sc2.B[0-1]+,sc1.sc1")
                        .build();

        assertAllowed(selectors, null, "sc1", "sc1");
        assertAllowed(selectors, null, "sc1", "A1");
        assertAllowed(selectors, null, "sc1", "A2");
        assertAllowed(selectors, null, "sc1", "A2");
        assertAllowed(selectors, null, "sc2", "B0");
        assertNotAllowed(selectors, "db", "sc1", "A1");
        assertNotAllowed(selectors, null, "sc1", "A");
        assertNotAllowed(selectors, null, "sc1", "AA");
        assertNotAllowed(selectors, null, "sc2", "B");
        assertNotAllowed(selectors, null, "sc2", "B2");
        assertNotAllowed(selectors, null, "sc11", "A1");
        assertNotAllowed(selectors, null, "sc1A", "A1");

        selectors = new Selectors.SelectorsBuilder().includeTables("sc1.sc1").build();
        assertAllowed(selectors, null, "sc1", "sc1");

        selectors = new Selectors.SelectorsBuilder().includeTables("sc1.sc[0-9]+").build();
        assertAllowed(selectors, null, "sc1", "sc1");

        selectors = new Selectors.SelectorsBuilder().includeTables("sc1.\\.*").build();
        assertAllowed(selectors, null, "sc1", "sc1");
    }

    @Test
    void testSchemaColumnCaseFormatDefaultTableInclusionsMatchAllIdShapes() {
        Selectors selectors =
                new Selectors.SelectorsBuilder()
                        .includeTables(
                                PipelineOptions.PIPELINE_COLUMN_NAME_CASE_DEFAULT_TABLE_INCLUSIONS)
                        .build();

        assertAllowed(selectors, null, null, "topic_only");
        assertAllowed(selectors, null, "saas_pw_00", "t_user");
        assertAllowed(selectors, "ns", "schema", "tbl");
    }

    /**
     * Deep validation of {@link
     * PipelineOptions#PIPELINE_COLUMN_NAME_CASE_DEFAULT_TABLE_INCLUSIONS}.
     *
     * <p>{@link Selectors.SelectorsBuilder#includeTables(String)} first splits on comma (with
     * escape rules), then splits each entry on <b>unescaped</b> {@code .}. Therefore a naive {@code
     * .*} as the one-part entry is invalid: the leading {@code .} is a segment separator, the
     * remainder is {@code *}, which is not a legal Java regex. Using {@code [\s\S]*} avoids any
     * unescaped dot while still matching any string (including newlines).
     */
    @Test
    void testPipelineSchemaColumnCaseFormatDefaultInclusionsCompileAndMatch() {
        String inclusions = PipelineOptions.PIPELINE_COLUMN_NAME_CASE_DEFAULT_TABLE_INCLUSIONS;

        String[] commaParts = Predicates.RegExSplitterByComma.split(inclusions);
        assertThat(commaParts).as("three comma-separated table patterns").hasSize(3);
        assertThat(commaParts[0]).isEqualTo("[\\s\\S]*");
        assertThat(commaParts[1]).isEqualTo("[\\s\\S]*.[\\s\\S]*");
        assertThat(commaParts[2]).isEqualTo("[\\s\\S]*.[\\s\\S]*.[\\s\\S]*");

        assertThatCode(
                        () -> {
                            for (String part : commaParts) {
                                String[] dotParts = Predicates.RegExSplitterByDot.split(part);
                                for (String seg : dotParts) {
                                    Pattern.compile(seg, Pattern.CASE_INSENSITIVE);
                                }
                            }
                        })
                .as("every segment must compile as a regex (CASE_INSENSITIVE like Selectors)")
                .doesNotThrowAnyException();

        assertThatThrownBy(() -> new Selectors.SelectorsBuilder().includeTables(".*").build())
                .isInstanceOf(PatternSyntaxException.class)
                .hasMessageContaining("Dangling meta character '*'");

        Selectors selectors = new Selectors.SelectorsBuilder().includeTables(inclusions).build();

        // --- 1-part: namespace & schema absent (e.g. Kafka topic) ---
        assertAllowed(selectors, null, null, "topic_only");
        assertAllowed(selectors, null, null, "events.v1.orders");
        assertAllowed(selectors, null, null, "topic-with-dashes");
        assertAllowed(selectors, null, null, "T_中文表名");

        // --- 2-part: schema + table (e.g. MySQL database.table) ---
        assertAllowed(selectors, null, "saas_pw_00", "t_user");
        assertAllowed(selectors, null, "db", "t");
        assertAllowed(selectors, null, "my_schema", "tbl_01");

        // --- 3-part: namespace + schema + table (e.g. Oracle) ---
        assertAllowed(selectors, "ns", "schema", "tbl");
        assertAllowed(selectors, "ORCL", "HR", "EMPLOYEES");

        // 3-part must not be forced through the 2-part selector only: still matches via 3rd arm
        assertAllowed(selectors, "catalog", "db", "table");
    }

    @Test
    void testDotSplittingDoesNotBreakTwoAndThreePartDefaultPatterns() {
        String inclusions = PipelineOptions.PIPELINE_COLUMN_NAME_CASE_DEFAULT_TABLE_INCLUSIONS;
        String[] commaParts = Predicates.RegExSplitterByComma.split(inclusions);

        String[] twoPartDots = Predicates.RegExSplitterByDot.split(commaParts[1]);
        assertThat(twoPartDots).containsExactly("[\\s\\S]*", "[\\s\\S]*");

        String[] threePartDots = Predicates.RegExSplitterByDot.split(commaParts[2]);
        assertThat(threePartDots).containsExactly("[\\s\\S]*", "[\\s\\S]*", "[\\s\\S]*");
    }

    @Test
    void testNegativeLookaheadWithoutWildcardDoesNotMatchRealTableNames() {
        Selectors selectors =
                new Selectors.SelectorsBuilder()
                        .includeTables(
                                "xxsc.(?!order_info|yearcard_team_detail|order_detail_model)")
                        .build();

        // The table segment is only a zero-width assertion. Since Selectors uses matches() for the
        // whole table name, no actual table name can match this pattern.
        assertNotAllowed(selectors, null, "xxsc", "customer");
        assertNotAllowed(selectors, null, "xxsc", "member_profile");
        assertNotAllowed(selectors, null, "xxsc", "order_info");
    }

    @Test
    void testNegativeLookaheadWildcardNeedsEscapedDotToSurviveSelectorParsing() {
        assertThatThrownBy(
                        () ->
                                new Selectors.SelectorsBuilder()
                                        .includeTables(
                                                "xxsc.(?!(order_info|yearcard_team_detail|order_detail_model)$).*")
                                        .build())
                .isInstanceOf(PatternSyntaxException.class)
                .hasMessageContaining("Dangling meta character '*'");
    }

    @Test
    void testEscapedNegativeLookaheadWildcardMatchesExpectedTables() {
        Selectors selectors =
                new Selectors.SelectorsBuilder()
                        .includeTables(
                                "xxsc.(?!(order_info|yearcard_team_detail|order_detail_model)$)\\.*")
                        .build();

        assertAllowed(selectors, null, "xxsc", "customer");
        assertAllowed(selectors, null, "xxsc", "member_profile");
        assertAllowed(selectors, null, "xxsc", "order_info_backup");
        assertNotAllowed(selectors, null, "xxsc", "order_info");
        assertNotAllowed(selectors, null, "xxsc", "yearcard_team_detail");
        assertNotAllowed(selectors, null, "xxsc", "order_detail_model");
    }

    protected void assertAllowed(
            Selectors filter, String nameSpace, String schemaName, String tableName) {

        TableId id = getTableId(nameSpace, schemaName, tableName);

        assertThat(filter.isMatch(id)).isTrue();
    }

    protected void assertNotAllowed(
            Selectors filter, String nameSpace, String schemaName, String tableName) {

        TableId id = getTableId(nameSpace, schemaName, tableName);

        assertThat(filter.isMatch(id)).isFalse();
    }

    private static TableId getTableId(String nameSpace, String schemaName, String tableName) {
        TableId id;
        if (nameSpace == null && schemaName == null) {
            id = TableId.tableId(tableName);
        } else if (nameSpace == null) {
            id = TableId.tableId(schemaName, tableName);
        } else {
            id = TableId.tableId(nameSpace, schemaName, tableName);
        }
        return id;
    }
}
