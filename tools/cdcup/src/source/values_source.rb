# frozen_string_literal: true

# Values source definition generator class.
class ValuesSource
  class << self
    def connector_name
      'flink-cdc-pipeline-connector-values'
    end

    # Nothing to do
    def prepend_to_docker_compose_yaml(_); end

    def prepend_to_pipeline_yaml(pipeline_yaml)
      pipeline_yaml['source'] = {
        'type' => 'values',
        'event-set.id' => 'SINGLE_SPLIT_MULTI_TABLES'
      }
    end
  end
end
