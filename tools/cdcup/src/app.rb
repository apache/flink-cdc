#!/usr/bin/env ruby
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

require 'tty-prompt'
require 'yaml'

require_relative 'download_libs'

require_relative 'source/my_sql'
require_relative 'source/values_source'

require_relative 'sink/doris'
require_relative 'sink/kafka'
require_relative 'sink/paimon'
require_relative 'sink/star_rocks'
require_relative 'sink/values_sink'

CDC_DATA_VOLUME = 'cdc-data'

@prompt = TTY::Prompt.new

@docker_compose_file_content = {
  'services' => {},
  'volumes' => {
    CDC_DATA_VOLUME => {}
  }
}

@pipeline_yaml_file_content = {
  'pipeline' => {
    'parallelism' => 1
  }
}

SOURCES = {
  mysql: MySQL,
  values: ValuesSource
}.freeze

SINKS = {
  kafka: Kafka,
  paimon: Paimon,
  doris: Doris,
  starrocks: StarRocks,
  values: ValuesSink
}.freeze

FLINK_VERSIONS = %w[
  1.17.2
  1.18.1
  1.19.2
  1.20.1
].freeze

FLINK_CDC_VERSIONS = %w[
  3.0.0
  3.0.1
  3.1.0
  3.1.1
  3.2.0
  3.2.1
].freeze

puts
@prompt.say '🎉 Welcome to cdc-up quickstart wizard!'
@prompt.say '   There are a few questions to ask before getting started:'

flink_version = @prompt.select('🐿️ Which Flink version would you like to use?', FLINK_VERSIONS,
                               default: FLINK_VERSIONS.last)
flink_cdc_version = @prompt.select('  ️ Which Flink CDC version would you like to use?', FLINK_CDC_VERSIONS,
                                   default: FLINK_CDC_VERSIONS.last)

@docker_compose_file_content['services']['jobmanager'] = {
  'image' => "flink:#{flink_version}-scala_2.12",
  'hostname' => 'jobmanager',
  'ports' => ['8081'],
  'command' => 'jobmanager',
  'environment' => {
    'FLINK_PROPERTIES' => "jobmanager.rpc.address: jobmanager\nexecution.checkpointing.interval: 3000"
  }
}

@docker_compose_file_content['services']['taskmanager'] = {
  'image' => "flink:#{flink_version}-scala_2.12",
  'hostname' => 'taskmanager',
  'command' => 'taskmanager',
  'environment' => {
    'FLINK_PROPERTIES' => "jobmanager.rpc.address: jobmanager\ntaskmanager.numberOfTaskSlots: 4\nexecution.checkpointing.interval: 3000"
  }
}

source = @prompt.select('🚰 Which data source to use?', SOURCES.keys)
sink = @prompt.select('🪣 Which data sink to use?', SINKS.keys)

SOURCES[source].prepend_to_docker_compose_yaml(@docker_compose_file_content)
SOURCES[source].prepend_to_pipeline_yaml(@pipeline_yaml_file_content)
SINKS[sink].prepend_to_docker_compose_yaml(@docker_compose_file_content)
SINKS[sink].prepend_to_pipeline_yaml(@pipeline_yaml_file_content)

File.write('/cdc/docker-compose.yaml', YAML.dump(@docker_compose_file_content))
File.write('/cdc/pipeline-definition.yaml', YAML.dump(@pipeline_yaml_file_content))

@prompt.say "\n3️⃣  Preparing CDC #{flink_cdc_version}..."

connectors_name = Set.new [
  SOURCES[source].connector_name,
  SINKS[sink].connector_name
]

download_cdc(flink_cdc_version, '/cdc/', connectors_name)

@prompt.say '🥳 All done!'
