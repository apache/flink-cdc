# frozen_string_literal: true

# Paimon sink definition generator class.
class Paimon
  class << self
    def connector_name
      'flink-cdc-pipeline-connector-paimon'
    end

    # Nothing to do
    def prepend_to_docker_compose_yaml(_); end

    def prepend_to_pipeline_yaml(pipeline_yaml)
      pipeline_yaml['sink'] = {
        'type' => 'paimon',
        'catalog.properties.metastore' => 'filesystem',
        'catalog.properties.warehouse' => '/data/paimon-warehouse'
      }
    end
  end
end
