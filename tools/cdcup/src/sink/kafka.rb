# frozen_string_literal: true
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Kafka sink definition generator class.
class Kafka
  class << self
    def connector_name
      'flink-cdc-pipeline-connector-kafka'
    end

    def prepend_to_docker_compose_yaml(docker_compose_yaml)
      docker_compose_yaml['services']['zookeeper'] = {
        'image' => 'confluentinc/cp-zookeeper:7.4.4',
        'hostname' => 'zookeeper',
        'ports' => ['2181'],
        'environment' => {
          'ZOOKEEPER_CLIENT_PORT' => 2181,
          'ZOOKEEPER_TICK_TIME' => 2000
        }
      }
      docker_compose_yaml['services']['kafka'] = {
        'image' => 'confluentinc/cp-kafka:7.4.4',
        'depends_on' => ['zookeeper'],
        'hostname' => 'kafka',
        'ports' => ['9092'],
        'environment' => {
          'KAFKA_BROKER_ID' => 1,
          'KAFKA_ZOOKEEPER_CONNECT' => 'zookeeper:2181',
          'KAFKA_ADVERTISED_LISTENERS' => 'PLAINTEXT://kafka:9092',
          'KAFKA_LISTENER_SECURITY_PROTOCOL_MAP' => 'PLAINTEXT:PLAINTEXT',
          'KAFKA_INTER_BROKER_LISTENER_NAME' => 'PLAINTEXT',
          'KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR' => 1
        }
      }
    end

    def prepend_to_pipeline_yaml(pipeline_yaml)
      pipeline_yaml['sink'] = {
        'type' => 'kafka',
        'properties.bootstrap.servers' => 'PLAINTEXT://kafka:9092'
      }
    end
  end
end
