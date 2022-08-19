/*
 * Copyright 2022 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.mysql.table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import com.ververica.cdc.connectors.mysql.source.MySqlSourceTestBase;
import com.ververica.cdc.connectors.mysql.testutils.UniqueDatabase;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

/** Test supporting different column charsets for MySQL Table source. */
@RunWith(Parameterized.class)
public class MysqlConnectorCharsetITCase extends MySqlSourceTestBase {

    private static final String TEST_USER = "mysqluser";
    private static final String TEST_PASSWORD = "mysqlpw";

    private static final UniqueDatabase charsetTestDatabase =
            new UniqueDatabase(MYSQL_CONTAINER, "charset_test", TEST_USER, TEST_PASSWORD);

    private final StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();

    private final StreamTableEnvironment tEnv =
            StreamTableEnvironment.create(
                    env, EnvironmentSettings.newInstance().inStreamingMode().build());

    private final String testName;
    private final String[] snapshotExpected;
    private final String[] binlogExpected;

    public MysqlConnectorCharsetITCase(
            String testName, String[] snapshotExpected, String[] binlogExpected) {
        this.testName = testName;
        this.snapshotExpected = snapshotExpected;
        this.binlogExpected = binlogExpected;
    }

    @Parameterized.Parameters(name = "Test column charset: {0}")
    public static Object[] parameters() {
        return new Object[][] {
            new Object[] {
                "ucs2_test",
                new String[] {"+I[1, 测试数据]", "+I[2, Craig Marshall]", "+I[3, 另一个测试数据]"},
                new String[] {
                    "-D[1, 测试数据]",
                    "-D[2, Craig Marshall]",
                    "-D[3, 另一个测试数据]",
                    "+I[11, 测试数据]",
                    "+I[12, Craig Marshall]",
                    "+I[13, 另一个测试数据]"
                }
            },
            new Object[] {
                "utf8_test",
                new String[] {"+I[1, 测试数据]", "+I[2, Craig Marshall]", "+I[3, 另一个测试数据]"},
                new String[] {
                    "-D[1, 测试数据]",
                    "-D[2, Craig Marshall]",
                    "-D[3, 另一个测试数据]",
                    "+I[11, 测试数据]",
                    "+I[12, Craig Marshall]",
                    "+I[13, 另一个测试数据]"
                }
            },
            new Object[] {
                "ascii_test",
                new String[] {"+I[1, ascii test!?]", "+I[2, Craig Marshall]", "+I[3, {test}]"},
                new String[] {
                    "-D[1, ascii test!?]",
                    "-D[2, Craig Marshall]",
                    "-D[3, {test}]",
                    "+I[11, ascii test!?]",
                    "+I[12, Craig Marshall]",
                    "+I[13, {test}]"
                }
            },
            new Object[] {
                "sjis_test",
                new String[] {"+I[1, ひびぴ]", "+I[2, Craig Marshall]", "+I[3, フブプ]"},
                new String[] {
                    "-D[1, ひびぴ]",
                    "-D[2, Craig Marshall]",
                    "-D[3, フブプ]",
                    "+I[11, ひびぴ]",
                    "+I[12, Craig Marshall]",
                    "+I[13, フブプ]"
                }
            },
            new Object[] {
                "gbk_test",
                new String[] {"+I[1, 测试数据]", "+I[2, Craig Marshall]", "+I[3, 另一个测试数据]"},
                new String[] {
                    "-D[1, 测试数据]",
                    "-D[2, Craig Marshall]",
                    "-D[3, 另一个测试数据]",
                    "+I[11, 测试数据]",
                    "+I[12, Craig Marshall]",
                    "+I[13, 另一个测试数据]"
                }
            },
            new Object[] {
                "cp932_test",
                new String[] {"+I[1, ひびぴ]", "+I[2, Craig Marshall]", "+I[3, フブプ]"},
                new String[] {
                    "-D[1, ひびぴ]",
                    "-D[2, Craig Marshall]",
                    "-D[3, フブプ]",
                    "+I[11, ひびぴ]",
                    "+I[12, Craig Marshall]",
                    "+I[13, フブプ]"
                }
            },
            new Object[] {
                "gb2312_test",
                new String[] {"+I[1, 测试数据]", "+I[2, Craig Marshall]", "+I[3, 另一个测试数据]"},
                new String[] {
                    "-D[1, 测试数据]",
                    "-D[2, Craig Marshall]",
                    "-D[3, 另一个测试数据]",
                    "+I[11, 测试数据]",
                    "+I[12, Craig Marshall]",
                    "+I[13, 另一个测试数据]"
                }
            },
            new Object[] {
                "ujis_test",
                new String[] {"+I[1, ひびぴ]", "+I[2, Craig Marshall]", "+I[3, フブプ]"},
                new String[] {
                    "-D[1, ひびぴ]",
                    "-D[2, Craig Marshall]",
                    "-D[3, フブプ]",
                    "+I[11, ひびぴ]",
                    "+I[12, Craig Marshall]",
                    "+I[13, フブプ]"
                }
            },
            new Object[] {
                "euckr_test",
                new String[] {"+I[1, 죠주쥬]", "+I[2, Craig Marshall]", "+I[3, 한국어]"},
                new String[] {
                    "-D[1, 죠주쥬]",
                    "-D[2, Craig Marshall]",
                    "-D[3, 한국어]",
                    "+I[11, 죠주쥬]",
                    "+I[12, Craig Marshall]",
                    "+I[13, 한국어]"
                }
            },
            new Object[] {
                "latin1_test",
                new String[] {"+I[1, ÀÆÉ]", "+I[2, Craig Marshall]", "+I[3, Üæû]"},
                new String[] {
                    "-D[1, ÀÆÉ]",
                    "-D[2, Craig Marshall]",
                    "-D[3, Üæû]",
                    "+I[11, ÀÆÉ]",
                    "+I[12, Craig Marshall]",
                    "+I[13, Üæû]"
                }
            },
            new Object[] {
                "latin2_test",
                new String[] {"+I[1, ÓÔŐÖ]", "+I[2, Craig Marshall]", "+I[3, ŠŞŤŹ]"},
                new String[] {
                    "-D[1, ÓÔŐÖ]",
                    "-D[2, Craig Marshall]",
                    "-D[3, ŠŞŤŹ]",
                    "+I[11, ÓÔŐÖ]",
                    "+I[12, Craig Marshall]",
                    "+I[13, ŠŞŤŹ]"
                }
            },
            new Object[] {
                "greek_test",
                new String[] {"+I[1, αβγδε]", "+I[2, Craig Marshall]", "+I[3, θικλ]"},
                new String[] {
                    "-D[1, αβγδε]",
                    "-D[2, Craig Marshall]",
                    "-D[3, θικλ]",
                    "+I[11, αβγδε]",
                    "+I[12, Craig Marshall]",
                    "+I[13, θικλ]"
                }
            },
            new Object[] {
                "hebrew_test",
                new String[] {"+I[1, בבקשה]", "+I[2, Craig Marshall]", "+I[3, שרפה]"},
                new String[] {
                    "-D[1, בבקשה]",
                    "-D[2, Craig Marshall]",
                    "-D[3, שרפה]",
                    "+I[11, בבקשה]",
                    "+I[12, Craig Marshall]",
                    "+I[13, שרפה]"
                }
            },
            new Object[] {
                "cp866_test",
                new String[] {"+I[1, твой]", "+I[2, Craig Marshall]", "+I[3, любой]"},
                new String[] {
                    "-D[1, твой]",
                    "-D[2, Craig Marshall]",
                    "-D[3, любой]",
                    "+I[11, твой]",
                    "+I[12, Craig Marshall]",
                    "+I[13, любой]"
                }
            },
            new Object[] {
                "tis620_test",
                new String[] {"+I[1, ภาษาไทย]", "+I[2, Craig Marshall]", "+I[3, ฆงจฉ]"},
                new String[] {
                    "-D[1, ภาษาไทย]",
                    "-D[2, Craig Marshall]",
                    "-D[3, ฆงจฉ]",
                    "+I[11, ภาษาไทย]",
                    "+I[12, Craig Marshall]",
                    "+I[13, ฆงจฉ]"
                }
            },
            new Object[] {
                "cp1250_test",
                new String[] {"+I[1, ÓÔŐÖ]", "+I[2, Craig Marshall]", "+I[3, ŠŞŤŹ]"},
                new String[] {
                    "-D[1, ÓÔŐÖ]",
                    "-D[2, Craig Marshall]",
                    "-D[3, ŠŞŤŹ]",
                    "+I[11, ÓÔŐÖ]",
                    "+I[12, Craig Marshall]",
                    "+I[13, ŠŞŤŹ]"
                }
            },
            new Object[] {
                "cp1251_test",
                new String[] {"+I[1, твой]", "+I[2, Craig Marshall]", "+I[3, любой]"},
                new String[] {
                    "-D[1, твой]",
                    "-D[2, Craig Marshall]",
                    "-D[3, любой]",
                    "+I[11, твой]",
                    "+I[12, Craig Marshall]",
                    "+I[13, любой]"
                }
            },
            new Object[] {
                "cp1257_test",
                new String[] {
                    "+I[1, piedzimst brīvi]", "+I[2, Craig Marshall]", "+I[3, apveltīti ar saprātu]"
                },
                new String[] {
                    "-D[1, piedzimst brīvi]", "-D[2, Craig Marshall]",
                            "-D[3, apveltīti ar saprātu]",
                    "+I[11, piedzimst brīvi]", "+I[12, Craig Marshall]",
                            "+I[13, apveltīti ar saprātu]"
                }
            },
            new Object[] {
                "macroman_test",
                new String[] {"+I[1, ÀÆÉ]", "+I[2, Craig Marshall]", "+I[3, Üæû]"},
                new String[] {
                    "-D[1, ÀÆÉ]",
                    "-D[2, Craig Marshall]",
                    "-D[3, Üæû]",
                    "+I[11, ÀÆÉ]",
                    "+I[12, Craig Marshall]",
                    "+I[13, Üæû]"
                }
            },
            new Object[] {
                "macce_test",
                new String[] {"+I[1, ÓÔŐÖ]", "+I[2, Craig Marshall]", "+I[3, ŮÚŰÜ]"},
                new String[] {
                    "-D[1, ÓÔŐÖ]",
                    "-D[2, Craig Marshall]",
                    "-D[3, ŮÚŰÜ]",
                    "+I[11, ÓÔŐÖ]",
                    "+I[12, Craig Marshall]",
                    "+I[13, ŮÚŰÜ]"
                }
            },
            new Object[] {
                "big5_test",
                new String[] {"+I[1, 大五]", "+I[2, Craig Marshall]", "+I[3, 丹店]"},
                new String[] {
                    "-D[1, 大五]",
                    "-D[2, Craig Marshall]",
                    "-D[3, 丹店]",
                    "+I[11, 大五]",
                    "+I[12, Craig Marshall]",
                    "+I[13, 丹店]"
                }
            }
        };
    }

    @BeforeClass
    public static void beforeClass() {
        charsetTestDatabase.createAndInitialize();
    }

    @Before
    public void before() {
        TestValuesTableFactory.clearAllData();
        env.setParallelism(DEFAULT_PARALLELISM);
        env.enableCheckpointing(200);
    }

    @Test
    public void testCharset() throws Exception {
        String sourceDDL =
                String.format(
                        "CREATE TABLE %s (\n"
                                + "  table_id BIGINT,\n"
                                + "  table_name STRING,\n"
                                + "  primary key(table_id) not enforced"
                                + ") WITH ("
                                + " 'connector' = 'mysql-cdc',"
                                + " 'hostname' = '%s',"
                                + " 'port' = '%s',"
                                + " 'username' = '%s',"
                                + " 'password' = '%s',"
                                + " 'database-name' = '%s',"
                                + " 'table-name' = '%s',"
                                + " 'scan.incremental.snapshot.enabled' = '%s',"
                                + " 'server-id' = '%s',"
                                + " 'server-time-zone' = 'UTC',"
                                + " 'scan.incremental.snapshot.chunk.size' = '%s'"
                                + ")",
                        testName,
                        MYSQL_CONTAINER.getHost(),
                        MYSQL_CONTAINER.getDatabasePort(),
                        charsetTestDatabase.getUsername(),
                        charsetTestDatabase.getPassword(),
                        charsetTestDatabase.getDatabaseName(),
                        testName,
                        true,
                        getServerId(),
                        4);
        tEnv.executeSql(sourceDDL);
        // async submit job
        TableResult result =
                tEnv.executeSql(String.format("SELECT table_id,table_name FROM %s", testName));

        // test snapshot phase
        CloseableIterator<Row> iterator = result.collect();
        waitForSnapshotStarted(iterator);
        assertEqualsInAnyOrder(
                Arrays.asList(snapshotExpected), fetchRows(iterator, snapshotExpected.length));

        // test binlog phase
        try (Connection connection = charsetTestDatabase.getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute(String.format("UPDATE %s SET table_id = table_id + 10;", testName));
        }
        assertEqualsInAnyOrder(
                Arrays.asList(binlogExpected), fetchRows(iterator, binlogExpected.length));
        result.getJobClient().ifPresent(client -> client.cancel());
    }

    private String getServerId() {
        final Random random = new Random();
        int serverId = random.nextInt(100) + 5400;
        return serverId + "-" + (serverId + env.getParallelism());
    }

    private static List<String> fetchRows(Iterator<Row> iter, int size) {
        List<String> rows = new ArrayList<>(size);
        while (size > 0 && iter.hasNext()) {
            Row row = iter.next();
            rows.add(row.toString());
            size--;
        }
        return rows;
    }

    private static void waitForSnapshotStarted(CloseableIterator<Row> iterator) throws Exception {
        while (!iterator.hasNext()) {
            Thread.sleep(100);
        }
    }
}
