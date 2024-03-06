<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# DataStream api 打包指南

本指南提供了 mysql cdc DataStream api 的简单 pom 示例

## 框架版本

flink 1.17.2  flink mysql cdc 2.4.2

## pom 示例

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>com.ververica</groupId>
    <artifactId>FlinkCDCTest</artifactId>
    <version>1.0-SNAPSHOT</version>

    <properties>
        <maven.compiler.source>8</maven.compiler.source>
        <maven.compiler.target>8</maven.compiler.target>
        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
        <java.version>1.8</java.version>
        <scala.binary.version>2.12</scala.binary.version>
        <maven.compiler.source>${java.version}</maven.compiler.source>
        <maven.compiler.target>${java.version}</maven.compiler.target>
        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
        <!-- Enforce single fork execution due to heavy mini cluster use in the tests -->
        <flink.forkCount>1</flink.forkCount>
        <flink.reuseForks>true</flink.reuseForks>

        <!-- dependencies versions -->
        <flink.version>1.17.2</flink.version>
        <slf4j.version>1.7.15</slf4j.version>
        <log4j.version>2.17.1</log4j.version>
        <debezium.version>1.9.7.Final</debezium.version>
    </properties>
    <dependencies>
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-streaming-java</artifactId>
            <version>${flink.version}</version>
            <scope>provided</scope>
        </dependency>
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-java</artifactId>
            <version>${flink.version}</version>
            <scope>provided</scope>
        </dependency>
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-clients</artifactId>
            <version>${flink.version}</version>
            <scope>provided</scope>
        </dependency>
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-table-planner_${scala.binary.version}</artifactId>
            <version>${flink.version}</version>
            <scope>provided</scope>
        </dependency>
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-table-runtime</artifactId>
            <version>${flink.version}</version>
            <scope>provided</scope>
        </dependency>
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-core</artifactId>
            <version>${flink.version}</version>
            <scope>provided</scope>
        </dependency>
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-table-common</artifactId>
            <version>${flink.version}</version>
            <scope>provided</scope>
        </dependency>
        <!-- Checked the dependencies of the Flink project and below is a feasible reference. -->
        <!--  Use flink shaded guava  18.0-13.0 for flink 1.13   -->
        <!--  Use flink shaded guava  30.1.1-jre-14.0 for flink-1.14  -->
        <!--  Use flink shaded guava  30.1.1-jre-15.0 for flink-1.15  -->
        <!--  Use flink shaded guava  30.1.1-jre-15.0 for flink-1.16  -->
        <!--  Use flink shaded guava  30.1.1-jre-16.1 for flink-1.17  -->
        <!--  Use flink shaded guava  31.1-jre-17.0   for flink-1.18  -->
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-shaded-guava</artifactId>
            <version>30.1.1-jre-16.1</version>
        </dependency>
        <dependency>
            <groupId>com.ververica</groupId>
            <artifactId>flink-connector-mysql-cdc</artifactId>
            <version>2.4.2</version>
        </dependency>
        <dependency>
            <groupId>io.debezium</groupId>
            <artifactId>debezium-connector-mysql</artifactId>
            <version>${debezium.version}</version>
        </dependency>
    </dependencies>

    <build>
        <plugins>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-shade-plugin</artifactId>
                <executions>
                    <execution>
                        <id>shade-flink</id>
                        <phase>package</phase>
                        <goals>
                            <goal>shade</goal>
                        </goals>
                        <configuration>
                            <!-- Shading test jar have bug in some previous version, so close this configuration here,
                            see https://issues.apache.org/jira/browse/MSHADE-284 -->
                            <shadeTestJar>false</shadeTestJar>
                            <shadedArtifactAttached>false</shadedArtifactAttached>
                            <createDependencyReducedPom>true</createDependencyReducedPom>
                            <dependencyReducedPomLocation>
                                ${project.basedir}/target/dependency-reduced-pom.xml
                            </dependencyReducedPomLocation>
                            <filters combine.children="append">
                                <filter>
                                    <artifact>*:*</artifact>
                                    <excludes>
                                        <exclude>module-info.class</exclude>
                                        <exclude>META-INF/*.SF</exclude>
                                        <exclude>META-INF/*.DSA</exclude>
                                        <exclude>META-INF/*.RSA</exclude>
                                    </excludes>
                                </filter>
                            </filters>
                            <artifactSet>
                                <includes>
                                    <!-- include nothing -->
                                    <include>io.debezium:debezium-api</include>
                                    <include>io.debezium:debezium-embedded</include>
                                    <include>io.debezium:debezium-core</include>
                                    <include>io.debezium:debezium-ddl-parser</include>
                                    <include>io.debezium:debezium-connector-mysql</include>
                                    <include>com.ververica:flink-connector-debezium</include>
                                    <include>com.ververica:flink-connector-mysql-cdc</include>
                                    <include>org.antlr:antlr4-runtime</include>
                                    <include>org.apache.kafka:*</include>
                                    <include>mysql:mysql-connector-java</include>
                                    <include>com.zendesk:mysql-binlog-connector-java</include>
                                    <include>com.fasterxml.*:*</include>
                                    <include>com.google.guava:*</include>
                                    <include>com.esri.geometry:esri-geometry-api</include>
                                    <include>com.zaxxer:HikariCP</include>
                                    <!--  Include fixed version 30.1.1-jre-16.0 of flink shaded guava  -->
                                    <include>org.apache.flink:flink-shaded-guava</include>
                                </includes>
                            </artifactSet>
                            <relocations>
                                <relocation>
                                    <pattern>org.apache.kafka</pattern>
                                    <shadedPattern>
                                        org.apache.flink.cdc.connectors.shaded.org.apache.kafka
                                    </shadedPattern>
                                </relocation>
                                <relocation>
                                    <pattern>org.antlr</pattern>
                                    <shadedPattern>
                                        org.apache.flink.cdc.connectors.shaded.org.antlr
                                    </shadedPattern>
                                </relocation>
                                <relocation>
                                    <pattern>com.fasterxml</pattern>
                                    <shadedPattern>
                                        org.apache.flink.cdc.connectors.shaded.com.fasterxml
                                    </shadedPattern>
                                </relocation>
                                <relocation>
                                    <pattern>com.google</pattern>
                                    <shadedPattern>
                                        org.apache.flink.cdc.connectors.shaded.com.google
                                    </shadedPattern>
                                </relocation>
                                <relocation>
                                    <pattern>com.esri.geometry</pattern>
                                    <shadedPattern>org.apache.flink.cdc.connectors.shaded.com.esri.geometry</shadedPattern>
                                </relocation>
                                <relocation>
                                    <pattern>com.zaxxer</pattern>
                                    <shadedPattern>
                                        org.apache.flink.cdc.connectors.shaded.com.zaxxer
                                    </shadedPattern>
                                </relocation>
                            </relocations>
                        </configuration>
                    </execution>
                </executions>
            </plugin>

        </plugins>
    </build>

</project>
```

## 代码示例

```java
package com.ververica.flink.cdc;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;

public class CdcTest {
    public static void main(String[] args) throws Exception {
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("yourHostname")
                .port(yourPort)
                .databaseList("yourDatabaseName") // 设置捕获的数据库， 如果需要同步整个数据库，请将 tableList 设置为 ".*".
                .tableList("yourDatabaseName.yourTableName") // 设置捕获的表
                .username("yourUsername")
                .password("yourPassword")
                .deserializer(new JsonDebeziumDeserializationSchema()) // 将 SourceRecord 转换为 JSON 字符串
                .build();

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置 3s 的 checkpoint 间隔
        env.enableCheckpointing(3000);

        env
                .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
                // 设置 source 节点的并行度为 1
                .setParallelism(1)
                .print().setParallelism(1); // 设置 sink 节点并行度为 1

        env.execute("Print MySQL Snapshot + Binlog");
    }
}

```