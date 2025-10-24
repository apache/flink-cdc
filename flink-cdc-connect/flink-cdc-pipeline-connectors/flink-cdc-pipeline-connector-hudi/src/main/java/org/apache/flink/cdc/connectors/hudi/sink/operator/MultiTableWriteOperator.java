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

package org.apache.flink.cdc.connectors.hudi.sink.operator;

import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.connectors.hudi.sink.function.MultiTableEventStreamWriteFunction;
import org.apache.flink.cdc.connectors.hudi.sink.v2.OperatorIDGenerator;
import org.apache.flink.cdc.runtime.operators.sink.SchemaEvolutionClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.TaskOperatorEventGateway;

import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.sink.common.AbstractWriteOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Multi-table write operator for Apache Hudi that handles CDC events from multiple tables. Extends
 * AbstractWriteOperator with Event as the input type to support CDC multi-table scenarios.
 *
 * <p>This operator:
 *
 * <ul>
 *   <li>Routes events to table-specific write functions
 *   <li>Maintains proper coordinator setup for each table
 *   <li>Passes events through to downstream operators
 *   <li>Handles schema evolution across multiple tables
 * </ul>
 */
public class MultiTableWriteOperator extends AbstractWriteOperator<Event> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(MultiTableWriteOperator.class);

    private final String schemaOperatorUid;
    private final MultiTableEventStreamWriteFunction multiTableWriteFunction;

    /**
     * Constructs a MultiTableWriteOperator.
     *
     * @param config Configuration for the operator
     */
    public MultiTableWriteOperator(Configuration config, String schemaOperatorUid) {
        this(config, schemaOperatorUid, new MultiTableEventStreamWriteFunction(config));
    }

    private MultiTableWriteOperator(
            Configuration config,
            String schemaOperatorUid,
            MultiTableEventStreamWriteFunction writeFunction) {
        super(writeFunction);
        this.schemaOperatorUid = schemaOperatorUid;
        this.multiTableWriteFunction = writeFunction;
    }

    @Override
    public void open() throws Exception {
        super.open();

        // Initialize SchemaEvolutionClient and set it on the MultiTableEventStreamWriteFunction
        TaskOperatorEventGateway toCoordinator =
                getContainingTask().getEnvironment().getOperatorCoordinatorEventGateway();
        OperatorID schemaOperatorID = new OperatorIDGenerator(schemaOperatorUid).generate();
        SchemaEvolutionClient schemaEvolutionClient =
                new SchemaEvolutionClient(toCoordinator, schemaOperatorID);

        // Set the SchemaEvolutionClient on the MultiTableEventStreamWriteFunction
        multiTableWriteFunction.setSchemaEvolutionClient(schemaEvolutionClient);

        // Register this sink subtask with the SchemaOperator
        int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
        try {
            schemaEvolutionClient.registerSubtask(subtaskIndex);
            LOG.info(
                    "Registered sink subtask {} with SchemaOperator {}",
                    subtaskIndex,
                    schemaOperatorUid);
        } catch (Exception e) {
            LOG.warn(
                    "Failed to register subtask with SchemaOperator, but continuing: {}",
                    e.getMessage());
        }
    }

    /**
     * Creates a MultiTableWriteOperatorFactory for multi-table Hudi write operations. This factory
     * uses our extended StreamWriteOperatorCoordinator for multi-table support.
     *
     * @param conf Configuration for the operator
     * @return MultiTableWriteOperatorFactory instance configured for multi-table support
     */
    public static MultiTableWriteOperatorFactory<Event> getFactory(
            Configuration conf, String schemaOperatorUid) {
        // Create coordinator-specific configuration with dummy table settings
        // This satisfies the coordinator's requirement for table initialization
        Configuration coordinatorConfig = createCoordinatorConfig(conf);

        LOG.info("Creating multi-table write operator factory with extended coordinator support");
        return MultiTableWriteOperatorFactory.instance(
                coordinatorConfig,
                new MultiTableWriteOperator(coordinatorConfig, schemaOperatorUid));
    }

    /**
     * Creates a coordinator-specific configuration with dummy table settings. This satisfies the
     * coordinator's requirement for table initialization while actual table routing happens
     * dynamically based on incoming events. Uses deterministic naming to allow reuse of existing
     * coordinator tables.
     */
    private static Configuration createCoordinatorConfig(Configuration originalConfig) {
        Configuration coordinatorConfig = new Configuration();
        coordinatorConfig.addAll(originalConfig);

        // Create deterministic dummy table name based on base path hash for reusability
        String originalPath = coordinatorConfig.get(FlinkOptions.PATH, "default");
        String pathHash = String.valueOf(Math.abs(originalPath.hashCode()));
        String dummyTableName = "coordinator_" + pathHash;

        coordinatorConfig.set(FlinkOptions.TABLE_NAME, dummyTableName);
        coordinatorConfig.set(FlinkOptions.DATABASE_NAME, "coordinator_db");

        // Set deterministic path for coordinator table (allows reuse)
        String coordinatorPath = originalPath + "/coordinator/" + dummyTableName;
        coordinatorConfig.set(FlinkOptions.PATH, coordinatorPath);

        // Set dummy Avro schema with a simple structure (id: int)
        String dummyAvroSchema =
                "{\n"
                        + "  \"type\": \"record\",\n"
                        + "  \"name\": \"coordinator_record\",\n"
                        + "  \"fields\": [\n"
                        + "    {\n"
                        + "      \"name\": \"id\",\n"
                        + "      \"type\": \"int\"\n"
                        + "    }\n"
                        + "  ]\n"
                        + "}";
        coordinatorConfig.set(FlinkOptions.SOURCE_AVRO_SCHEMA, dummyAvroSchema);

        LOG.info(
                "Created coordinator config with reusable dummy table: coordinator_db.{} at path: {}",
                dummyTableName,
                coordinatorPath);
        return coordinatorConfig;
    }
}
