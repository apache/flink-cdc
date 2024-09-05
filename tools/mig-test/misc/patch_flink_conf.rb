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

FLINK_HOME = ENV['FLINK_HOME']
throw 'Unspecified `FLINK_HOME` environment variable.' if FLINK_HOME.nil?

EXTRA_CONF = <<~EXTRACONF

taskmanager.numberOfTaskSlots: 10
parallelism.default: 4
execution.checkpointing.interval: 300
EXTRACONF

File.write("#{FLINK_HOME}/conf/flink-conf.yaml", EXTRA_CONF, mode: 'a+')

# MySQL connector is not provided
`wget https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.27/mysql-connector-java-8.0.27.jar -O #{FLINK_HOME}/lib/mysql-connector-java-8.0.27.jar`