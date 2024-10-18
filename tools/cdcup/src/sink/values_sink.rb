# frozen_string_literal: true

# Values sink definition generator class.
class ValuesSink
  class << self
    def connector_name
      'flink-cdc-pipeline-connector-values'
    end

    # Nothing to do
    def prepend_to_docker_compose_yaml(_); end

    def prepend_to_pipeline_yaml(pipeline_yaml)
      pipeline_yaml['sink'] = {
        'type' => 'values'
      }
    end
  end
end
