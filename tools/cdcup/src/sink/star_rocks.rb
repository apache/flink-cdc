# frozen_string_literal: true

# StarRocks sink definition generator class.
class StarRocks
  class << self
    def connector_name
      'flink-cdc-pipeline-connector-starrocks'
    end

    def prepend_to_docker_compose_yaml(docker_compose_yaml)
      docker_compose_yaml['services']['starrocks'] = {
        'image' => 'starrocks/allin1-ubuntu:3.2.6',
        'hostname' => 'starrocks',
        'ports' => %w[8080 9030],
        'volumes' => ["#{CDC_DATA_VOLUME}:/data"]
      }
    end

    def prepend_to_pipeline_yaml(pipeline_yaml)
      pipeline_yaml['sink'] = {
        'type' => 'starrocks',
        'jdbc-url' => 'jdbc:mysql://starrocks:9030',
        'load-url' => 'starrocks:8030',
        'username' => 'root',
        'password' => '',
        'table.create.properties.replication_num' => 1
      }
    end
  end
end
