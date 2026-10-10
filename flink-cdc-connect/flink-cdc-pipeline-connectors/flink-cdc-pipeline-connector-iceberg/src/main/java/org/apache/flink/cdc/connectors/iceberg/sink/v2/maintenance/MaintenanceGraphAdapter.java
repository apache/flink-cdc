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

package org.apache.flink.cdc.connectors.iceberg.sink.v2.maintenance;

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.sink.SupportsStreamGraphPostProcessing;
import org.apache.flink.cdc.runtime.operators.StreamNodeAdapter;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.ProcessOperator;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.util.InstantiationUtil;

import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.maintenance.api.TaskResult;
import org.apache.iceberg.flink.maintenance.operator.DataFileRewriteCommitter;
import org.apache.iceberg.flink.maintenance.operator.DataFileRewritePlanner;
import org.apache.iceberg.flink.maintenance.operator.DataFileRewriteRunner;
import org.apache.iceberg.flink.maintenance.operator.DeleteFilesProcessor;
import org.apache.iceberg.flink.maintenance.operator.ExpireSnapshotsProcessor;
import org.apache.iceberg.flink.maintenance.operator.FileNameReader;
import org.apache.iceberg.flink.maintenance.operator.ListFileSystemFiles;
import org.apache.iceberg.flink.maintenance.operator.ListMetadataFiles;
import org.apache.iceberg.flink.maintenance.operator.MetadataTablePlanner;
import org.apache.iceberg.flink.maintenance.operator.OrphanFilesDetector;
import org.apache.iceberg.flink.maintenance.operator.SkipOnError;
import org.apache.iceberg.flink.maintenance.operator.TaskResultAggregator;

import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.flink.cdc.common.utils.Preconditions.checkState;

/** Adapts initialization and resource ownership in Iceberg's generated maintenance graph. */
@Internal
public final class MaintenanceGraphAdapter
        implements SupportsStreamGraphPostProcessing, Serializable {
    private static final long serialVersionUID = 1L;
    private static final Pattern TASK_UID =
            Pattern.compile(
                    "(?:RewriteDataFiles|ExpireSnapshots|DeleteOrphanFiles) \\[\\d+\\]_\\d+_");

    // Registration happens while Flink expands the sink, and is only needed on the client.
    private transient Map<String, Task> tasks;

    void startTopology() {
        // Execution-plan inspection may expand the same sink before the submission build.
        tasks = new LinkedHashMap<>();
    }

    void register(
            String uid,
            String tableName,
            String taskName,
            int index,
            TableLoader loader,
            DeferredTableLoader deletionLoader,
            int deleteBatchSize) {
        if (tasks == null) {
            tasks = new LinkedHashMap<>();
        }
        Task previous =
                tasks.putIfAbsent(
                        uid,
                        new Task(
                                tableName,
                                taskName,
                                index,
                                loader,
                                deletionLoader,
                                deleteBatchSize));
        checkState(
                previous == null || previous.loader == loader,
                String.format(
                        Locale.ROOT, "Maintenance task UID is registered more than once: %s", uid));
    }

    @Override
    public void postProcessStreamGraph(StreamGraph graph) {
        if (tasks == null) {
            return;
        }
        Map<String, Task> owners = new LinkedHashMap<>();
        Map<String, Class<?>> expectedTypes = new HashMap<>();
        tasks.forEach(
                (suffix, task) ->
                        task.operators()
                                .forEach(
                                        (name, type) -> {
                                            owners.put(name + suffix, task);
                                            expectedTypes.put(name + suffix, type);
                                        }));
        Map<String, StreamNode> matched = new HashMap<>();
        for (StreamNode node : graph.getStreamNodes()) {
            String actualUid = node.getTransformationUID();
            if (actualUid == null) {
                continue;
            }
            // Flink prefixes post-commit operator UIDs with the enclosing sink UID.
            String candidate = actualUid;
            while (!owners.containsKey(candidate) && candidate.contains(": ")) {
                candidate = candidate.substring(candidate.indexOf(": ") + 2);
            }
            if (owners.containsKey(candidate)) {
                checkState(
                        matched.put(candidate, node) == null,
                        "Ambiguous native maintenance operator UID: " + candidate);
            } else {
                Matcher matcher = TASK_UID.matcher(actualUid);
                while (matcher.find()) {
                    checkState(
                            !tasks.containsKey(actualUid.substring(matcher.start())),
                            "Unexpected native maintenance operator: " + actualUid);
                }
            }
        }
        // Validate the complete task set before replacing any operators.
        for (String uid : owners.keySet()) {
            checkState(matched.containsKey(uid), "Missing native maintenance operator: " + uid);
        }
        owners.forEach((uid, task) -> adapt(matched.get(uid), expectedTypes.get(uid), task));
    }

    private static void adapt(StreamNode node, Class<?> expected, Task task) {
        checkState(
                node.getOperatorFactory() instanceof SimpleOperatorFactory,
                "Unsupported native maintenance operator factory: " + node.getTransformationUID());
        SimpleOperatorFactory<?> factory = (SimpleOperatorFactory<?>) node.getOperatorFactory();
        StreamOperator<?> operator = factory.getOperator();
        Object implementation =
                operator instanceof AbstractUdfStreamOperator
                        ? ((AbstractUdfStreamOperator<?, ?>) operator).getUserFunction()
                        : operator;
        if (implementation instanceof DeferredProcessFunction) {
            implementation = ((DeferredProcessFunction<?, ?>) implementation).delegate();
            checkState(implementation.getClass() == expected, "Unexpected adapted operator");
            return;
        }
        if ((expected == DeleteFilesProcessor.class
                        && operator instanceof ClosingDeleteFilesProcessor)
                || (expected == DataFileRewriteCommitter.class
                        && operator instanceof ClosingRewriteCommitter)) {
            return;
        }
        checkState(
                implementation.getClass() == expected,
                String.format(
                        Locale.ROOT,
                        "Iceberg maintenance operator changed for %s: expected %s but found %s",
                        node.getTransformationUID(),
                        expected.getName(),
                        implementation.getClass().getName()));

        StreamOperator<?> replacement;
        if (expected == DeleteFilesProcessor.class) {
            replacement =
                    new ClosingDeleteFilesProcessor(
                            task.deletionLoader.clone(),
                            task.taskName,
                            task.index,
                            task.deleteBatchSize);
        } else if (expected == DataFileRewriteCommitter.class) {
            replacement =
                    new ClosingRewriteCommitter(
                            task.tableName, task.taskName, task.index, task.loader.clone());
        } else if (operator instanceof ProcessOperator) {
            replacement = defer((ProcessFunction<?, ?>) implementation, task.loader);
        } else {
            // Stateful aggregators and orphan detection retain their native lifecycle and state.
            return;
        }
        SimpleOperatorFactory<?> replacementFactory = SimpleOperatorFactory.of(replacement);
        replacementFactory.setChainingStrategy(factory.getChainingStrategy());
        StreamNodeAdapter.setOperatorFactory(node, replacementFactory);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static ProcessOperator<?, ?> defer(ProcessFunction function, TableLoader loader) {
        Class<?> output;
        if (function instanceof DataFileRewritePlanner) {
            output = DataFileRewritePlanner.PlannedGroup.class;
        } else if (function instanceof DataFileRewriteRunner) {
            output = DataFileRewriteRunner.ExecutedGroup.class;
        } else if (function instanceof ExpireSnapshotsProcessor) {
            output = TaskResult.class;
        } else if (function instanceof MetadataTablePlanner) {
            output = MetadataTablePlanner.SplitInfo.class;
        } else {
            checkState(
                    function instanceof FileNameReader
                            || function instanceof ListMetadataFiles
                            || function instanceof ListFileSystemFiles,
                    "Unsupported maintenance function: " + function.getClass().getName());
            output = String.class;
        }
        try {
            // Clone the function and its SAME loader together. Each operator owns an independent
            // catalog, even before Flink serializes individual operators in an operator chain.
            DeferredProcessFunction<?, ?> isolated =
                    InstantiationUtil.clone(
                            new DeferredProcessFunction(function, output, loader),
                            MaintenanceGraphAdapter.class.getClassLoader());
            return new ProcessOperator(isolated);
        } catch (Exception e) {
            throw new IllegalStateException("Cannot isolate native maintenance function", e);
        }
    }

    private static final class Task {
        private final String tableName;
        private final String taskName;
        private final int index;
        private final TableLoader loader;
        private final DeferredTableLoader deletionLoader;
        private final int deleteBatchSize;

        private Task(
                String tableName,
                String taskName,
                int index,
                TableLoader loader,
                DeferredTableLoader deletionLoader,
                int deleteBatchSize) {
            this.tableName = tableName;
            this.taskName = taskName;
            this.index = index;
            this.loader = loader;
            this.deletionLoader = deletionLoader;
            this.deleteBatchSize = deleteBatchSize;
        }

        private Map<String, Class<?>> operators() {
            Map<String, Class<?>> result = new LinkedHashMap<>();
            if (taskName.startsWith("RewriteDataFiles [")) {
                result.put("RDF Planner", DataFileRewritePlanner.class);
                result.put("Rewrite", DataFileRewriteRunner.class);
                result.put("Rewrite commit", DataFileRewriteCommitter.class);
                result.put("Rewrite aggregator", TaskResultAggregator.class);
            } else if (taskName.startsWith("ExpireSnapshots [")) {
                result.put("Expire Snapshot", ExpireSnapshotsProcessor.class);
                result.put("Delete file", DeleteFilesProcessor.class);
            } else {
                checkState(
                        taskName.startsWith("DeleteOrphanFiles ["),
                        "Unknown maintenance task: " + taskName);
                result.put("Table Planner", MetadataTablePlanner.class);
                result.put("Files Reader", FileNameReader.class);
                result.put("List metadata Files", ListMetadataFiles.class);
                result.put("Filesystem Files", ListFileSystemFiles.class);
                result.put("Filter File", OrphanFilesDetector.class);
                result.put("Skip On Error", SkipOnError.class);
                result.put("Delete File", DeleteFilesProcessor.class);
                result.put("Orphan Files Aggregator", TaskResultAggregator.class);
            }
            return result;
        }
    }
}
