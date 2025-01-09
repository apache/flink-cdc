#!/usr/bin/env ruby
# frozen_string_literal: true

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

CDC_SOURCE_HOME = ENV['CDC_SOURCE_HOME']
throw 'Unspecified `CDC_SOURCE_HOME` environment variable.' if CDC_SOURCE_HOME.nil?

Dir.chdir(__dir__)

def gen_version(tag)
  {
    tar: "https://dlcdn.apache.org/flink/flink-cdc-#{tag}/flink-cdc-#{tag}-bin.tar.gz",
    connectors: %W[
      https://repo1.maven.org/maven2/org/apache/flink/flink-cdc-pipeline-connector-mysql/#{tag}/flink-cdc-pipeline-connector-mysql-#{tag}.jar
      https://repo1.maven.org/maven2/org/apache/flink/flink-cdc-pipeline-connector-doris/#{tag}/flink-cdc-pipeline-connector-doris-#{tag}.jar
      https://repo1.maven.org/maven2/org/apache/flink/flink-cdc-pipeline-connector-starrocks/#{tag}/flink-cdc-pipeline-connector-starrocks-#{tag}.jar
      https://repo1.maven.org/maven2/org/apache/flink/flink-cdc-pipeline-connector-kafka/#{tag}/flink-cdc-pipeline-connector-kafka-#{tag}.jar
      https://repo1.maven.org/maven2/org/apache/flink/flink-cdc-pipeline-connector-paimon/#{tag}/flink-cdc-pipeline-connector-paimon-#{tag}.jar
      https://repo1.maven.org/maven2/org/apache/flink/flink-cdc-pipeline-connector-values/#{tag}/flink-cdc-pipeline-connector-values-#{tag}.jar
    ]
  }
end

RELEASED_VERSIONS = {
  '3.2.0': gen_version('3.2.0'),
  '3.2.1': gen_version('3.2.1'),
}.freeze

HEAD_VERSION = '3.3-SNAPSHOT'

def download_or_get(link)
  `mkdir -p cache`
  file_name = "cache/#{File.basename(link)}"
  if File.exist? file_name
    puts "#{file_name} exists, skip download"
    return file_name
  end
  `wget #{link} -O #{file_name}`
  file_name
end

M2_REPO = '~/.m2/repository/org/apache/flink'
FILES = %w[
  dist
  pipeline-connector-kafka
  pipeline-connector-mysql
  pipeline-connector-doris
  pipeline-connector-paimon
  pipeline-connector-starrocks
  pipeline-connector-values
].freeze
def download_released
  `rm -rf cdc-versions`
  RELEASED_VERSIONS.each do |version, links|
    `mkdir -p cdc-versions/#{version}`
    file_name = download_or_get(links[:tar])
    `tar --strip-components=1 -xzvf #{file_name} -C cdc-versions/#{version}`
    links[:connectors].each do |link|
      jar_file_name = download_or_get(link)
      `cp #{jar_file_name} cdc-versions/#{version}/lib/`
    end
  end
end

def compile_snapshot(version)
  puts "Trying to create #{version}"
  `mkdir -p cdc-versions/#{version}/lib`
  `cp -r #{CDC_SOURCE_HOME}/flink-cdc-dist/src/main/flink-cdc-bin/* cdc-versions/#{version}/`

  puts 'Compiling snapshot version...'
  puts `cd #{CDC_SOURCE_HOME} && mvn clean install -DskipTests`

  FILES.each do |lib|
    if lib == 'dist'
      `cp #{CDC_SOURCE_HOME}/flink-cdc-#{lib}/target/flink-cdc-#{lib}-#{version}.jar cdc-versions/#{version}/lib/`
    else
      `cp #{CDC_SOURCE_HOME}/flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-#{lib}/target/flink-cdc-#{lib}-#{version}.jar cdc-versions/#{version}/lib/`
    end
  end
end

download_released
compile_snapshot HEAD_VERSION

puts 'Done'
