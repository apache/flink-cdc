/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.connectors.hudi.sink;

import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.factories.DataSinkFactory;
import org.apache.flink.cdc.common.factories.FactoryHelper;
import org.apache.flink.cdc.common.pipeline.PipelineOptions;

import org.apache.hudi.index.HoodieIndex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.cdc.connectors.hudi.sink.HudiDataSinkOptions.PREFIX_CATALOG_PROPERTIES;
import static org.apache.flink.cdc.connectors.hudi.sink.HudiDataSinkOptions.PREFIX_TABLE_PROPERTIES;

/**
 * Factory for creating {@link HudiDataSink}. This class defines the configuration options and
 * instantiates the sink by delegating option definitions to {@link HudiConfig}.
 */
public class HudiDataSinkFactory implements DataSinkFactory {

    private static final Logger LOG = LoggerFactory.getLogger(HudiDataSinkFactory.class);

    public static final String IDENTIFIER = "hudi";

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public HudiDataSink createDataSink(Context context) {
        LOG.info("Creating HudiDataSink for {}", context);

        FactoryHelper.createFactoryHelper(this, context)
                .validateExcept(PREFIX_TABLE_PROPERTIES, PREFIX_CATALOG_PROPERTIES);

        FactoryHelper.DefaultContext factoryContext = (FactoryHelper.DefaultContext) context;
        Configuration config = factoryContext.getFactoryConfiguration();

        // Validate that only BUCKET index type is used
        String indexType = config.get(HudiConfig.INDEX_TYPE);
        if (indexType != null && !indexType.equalsIgnoreCase(HoodieIndex.IndexType.BUCKET.name())) {
            throw new IllegalArgumentException(
                    String.format(
                            "Unsupported index type '%s'. Currently only 'BUCKET' index type is supported. "
                                    + "Other index types (e.g., FLINK_STATE, BLOOM, SIMPLE) are not yet implemented "
                                    + "for multi-table CDC pipelines.",
                            indexType));
        }
        config.set(HudiConfig.INDEX_TYPE, HoodieIndex.IndexType.BUCKET.name());

        String schemaOperatorUid =
                context.getPipelineConfiguration()
                        .get(PipelineOptions.PIPELINE_SCHEMA_OPERATOR_UID);

        return new HudiDataSink(config, schemaOperatorUid);
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(HudiConfig.PATH);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(HudiConfig.TABLE_TYPE);
        options.add(HudiConfig.INDEX_TYPE);
        return options;
    }
}
