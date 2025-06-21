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

package org.apache.flink.cdc.composer.definition;

import org.apache.flink.cdc.common.annotation.VisibleForTesting;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.pipeline.RuntimeExecutionMode;
import org.apache.flink.cdc.common.types.LocalZonedTimestampType;
import org.apache.flink.cdc.composer.PipelineComposer;
import org.apache.flink.cdc.composer.PipelineExecution;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.TimeZone;

import static org.apache.flink.cdc.common.pipeline.PipelineOptions.PIPELINE_EXECUTION_RUNTIME_MODE;
import static org.apache.flink.cdc.common.pipeline.PipelineOptions.PIPELINE_LOCAL_TIME_ZONE;

/**
 * Definition of a pipeline.
 *
 * <p>A pipeline consists of following components:
 *
 * <ul>
 *   <li>Source: data source of the pipeline. Required in the definition.
 *   <li>Sink: data destination of the pipeline. Required in the definition.
 *   <li>Routes: routers specifying the connection between source tables and sink tables. Optional
 *       in the definition.
 *   <li>Transforms: transformations for applying modifications to data change events. Optional in
 *       the definition.
 *   <li>Config: configurations of the pipeline. Optional in the definition.
 * </ul>
 *
 * <p>This class keeps track of the raw pipeline definition made by users via pipeline definition
 * file. A definition will be translated to a {@link PipelineExecution} by {@link PipelineComposer}
 * before being submitted to the computing engine.
 */
public class PipelineDef {
    private final SourceDef source;
    private final SinkDef sink;
    private final List<RouteDef> routes;
    private final List<TransformDef> transforms;
    private final List<UdfDef> udfs;
    private final List<ModelDef> models;
    private final Configuration config;

    public PipelineDef(
            SourceDef source,
            SinkDef sink,
            List<RouteDef> routes,
            List<TransformDef> transforms,
            List<UdfDef> udfs,
            List<ModelDef> models,
            Configuration config) {
        this.source = source;
        this.sink = sink;
        this.routes = routes;
        this.transforms = transforms;
        this.udfs = udfs;
        this.models = models;
        this.config = evaluatePipelineRuntimeExecutionMode(evaluatePipelineTimeZone(config));
    }

    public PipelineDef(
            SourceDef source,
            SinkDef sink,
            List<RouteDef> routes,
            List<TransformDef> transforms,
            List<UdfDef> udfs,
            Configuration config) {
        this(source, sink, routes, transforms, udfs, new ArrayList<>(), config);
    }

    public SourceDef getSource() {
        return source;
    }

    public SinkDef getSink() {
        return sink;
    }

    public List<RouteDef> getRoute() {
        return routes;
    }

    public List<TransformDef> getTransforms() {
        return transforms;
    }

    public List<UdfDef> getUdfs() {
        return udfs;
    }

    public List<ModelDef> getModels() {
        return models;
    }

    public Configuration getConfig() {
        return config;
    }

    @Override
    public String toString() {
        return "PipelineDef{"
                + "source="
                + source
                + ", sink="
                + sink
                + ", routes="
                + routes
                + ", transforms="
                + transforms
                + ", udfs="
                + udfs
                + ", models="
                + models
                + ", config="
                + config
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PipelineDef that = (PipelineDef) o;
        return Objects.equals(source, that.source)
                && Objects.equals(sink, that.sink)
                && Objects.equals(routes, that.routes)
                && Objects.equals(transforms, that.transforms)
                && Objects.equals(udfs, that.udfs)
                && Objects.equals(models, that.models)
                && Objects.equals(config, that.config);
    }

    @Override
    public int hashCode() {
        return Objects.hash(source, sink, routes, transforms, udfs, models, config);
    }

    // ------------------------------------------------------------------------
    //  Utilities
    // ------------------------------------------------------------------------

    /**
     * Returns the current session time zone id. It is used when converting to/from {@code TIMESTAMP
     * WITH LOCAL TIME ZONE}.
     *
     * @see LocalZonedTimestampType
     */
    @VisibleForTesting
    private static Configuration evaluatePipelineTimeZone(Configuration configuration) {
        final String zone = configuration.get(PIPELINE_LOCAL_TIME_ZONE);
        ZoneId zoneId;
        if (PIPELINE_LOCAL_TIME_ZONE.defaultValue().equals(zone)) {
            zoneId = ZoneId.systemDefault();
        } else {
            validateTimeZone(zone);
            zoneId = ZoneId.of(zone);
        }
        configuration.set(PIPELINE_LOCAL_TIME_ZONE, zoneId.toString());
        return configuration;
    }

    /**
     * Returns Runtime execution mode of the pipeline includes STREAMING and BATCH, with the default
     * value being STREAMING.
     */
    private static Configuration evaluatePipelineRuntimeExecutionMode(Configuration configuration) {
        if (!configuration.contains(PIPELINE_EXECUTION_RUNTIME_MODE)) {
            configuration.set(PIPELINE_EXECUTION_RUNTIME_MODE, RuntimeExecutionMode.STREAMING);
        }
        return configuration;
    }

    /**
     * Validates a time zone is valid or not.
     *
     * @param zone given time zone
     */
    private static void validateTimeZone(String zone) {
        boolean isValid;
        try {
            isValid = TimeZone.getTimeZone(zone).toZoneId().equals(ZoneId.of(zone));
        } catch (Exception ignore) {
            isValid = false;
        }

        if (!isValid) {
            throw new IllegalArgumentException(
                    "Invalid time zone. The valid value should be a Time Zone Database ID "
                            + "such as 'America/Los_Angeles' to include daylight saving time. "
                            + "Fixed offsets are supported using 'GMT-08:00' or 'GMT+08:00'. "
                            + "Or use 'UTC' without time zone and daylight saving time.");
        }
    }
}
