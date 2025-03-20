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
# Doris sink definition generator class.
class Doris
  class << self
    def connector_name
      'flink-cdc-pipeline-connector-doris'
    end

    def prepend_to_docker_compose_yaml(docker_compose_yaml)
      docker_compose_yaml['services']['doris'] = {
        'image' => 'apache/doris:doris-all-in-one-2.1.0',
        'hostname' => 'doris',
        'ports' => %w[8030 8040 9030],
        'volumes' => ["#{CDC_DATA_VOLUME}:/data"]
      }
    end

    def prepend_to_pipeline_yaml(pipeline_yaml)
      pipeline_yaml['sink'] = {
        'type' => 'doris',
        'fenodes' => 'doris:8030',
        'benodes' => 'doris:8040',
        'jdbc-url' => 'jdbc:mysql://doris:9030',
        'username' => 'root',
        'password' => '',
        'table.create.properties.light_schema_change' => true,
        'table.create.properties.replication_num' => 1
      }
    end
  end
end
