# frozen_string_literal: true

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
