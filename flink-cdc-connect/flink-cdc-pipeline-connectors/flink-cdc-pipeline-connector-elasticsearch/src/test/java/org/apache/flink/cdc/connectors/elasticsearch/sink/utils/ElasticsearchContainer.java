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

package org.apache.flink.cdc.connectors.elasticsearch.sink.utils;

import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;

/** Docker container for Elasticsearch. */
public class ElasticsearchContainer
        extends org.testcontainers.elasticsearch.ElasticsearchContainer {

    private static final String ELASTICSEARCH_IMAGE =
            "docker.elastic.co/elasticsearch/elasticsearch";

    public ElasticsearchContainer(String version) {
        this(DockerImageName.parse(String.format("%s:%s", ELASTICSEARCH_IMAGE, version)));
    }

    public ElasticsearchContainer(DockerImageName imageName) {
        super(imageName);
        withEnv("discovery.type", "single-node");
        withEnv("xpack.security.enabled", "false");
        withEnv("ES_JAVA_OPTS", "-Xms2g -Xmx2g");
        withEnv("logger.org.elasticsearch", "ERROR");
        waitingFor(Wait.forListeningPort().withStartupTimeout(Duration.ofMinutes(5)));
    }
}
