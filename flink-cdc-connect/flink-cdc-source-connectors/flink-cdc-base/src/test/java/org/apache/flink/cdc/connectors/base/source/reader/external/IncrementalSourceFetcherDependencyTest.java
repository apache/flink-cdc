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

package org.apache.flink.cdc.connectors.base.source.reader.external;

import org.apache.flink.cdc.connectors.base.config.SourceConfig;
import org.apache.flink.cdc.connectors.base.dialect.DataSourceDialect;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.base.source.meta.offset.Offset;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase;

import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/** Tests that incremental source fetchers do not depend on Flink's Guava 31 shade. */
class IncrementalSourceFetcherDependencyTest {

    private static final String SHADED_GUAVA31_PREFIX = "org.apache.flink.shaded.guava31.";

    @Test
    void incrementalFetchersCanBeConstructedWithoutFlinkShadedGuava31() throws Exception {
        FetchTask.Context context = new TestingFetchTaskContext();

        URL classesUrl = Paths.get("target/classes").toAbsolutePath().toUri().toURL();
        try (Guava31BlockingClassLoader classLoader =
                new Guava31BlockingClassLoader(
                        new URL[] {classesUrl}, getClass().getClassLoader())) {
            assertThatCode(
                            () ->
                                    constructAndCloseFetcher(
                                            classLoader,
                                            "org.apache.flink.cdc.connectors.base.source.reader.external.IncrementalSourceScanFetcher",
                                            context))
                    .doesNotThrowAnyException();
            assertThatCode(
                            () ->
                                    constructAndCloseFetcher(
                                            classLoader,
                                            "org.apache.flink.cdc.connectors.base.source.reader.external.IncrementalSourceStreamFetcher",
                                            context))
                    .doesNotThrowAnyException();
        }
    }

    @Test
    void productionClassesDoNotReferenceFlinkShadedGuava31() throws Exception {
        List<Path> classesReferencingFlinkShadedGuava31;
        try (Stream<Path> classes = Files.walk(Paths.get("target/classes"))) {
            classesReferencingFlinkShadedGuava31 =
                    classes.filter(path -> path.toString().endsWith(".class"))
                            .filter(this::containsFlinkShadedGuava31Reference)
                            .collect(Collectors.toList());
        }

        assertThat(classesReferencingFlinkShadedGuava31)
                .as("Production classes should not depend on Flink's Guava 31 shade")
                .isEmpty();
    }

    private boolean containsFlinkShadedGuava31Reference(Path path) {
        try {
            return new String(Files.readAllBytes(path), StandardCharsets.ISO_8859_1)
                    .contains("org/apache/flink/shaded/guava31");
        } catch (Exception e) {
            throw new AssertionError("Failed to inspect class file " + path, e);
        }
    }

    private static void constructAndCloseFetcher(
            ClassLoader classLoader, String className, FetchTask.Context context) throws Exception {
        Class<?> fetcherClass = Class.forName(className, true, classLoader);
        Constructor<?> constructor =
                fetcherClass.getConstructor(FetchTask.Context.class, int.class);
        Object fetcher = constructor.newInstance(context, 0);
        Method close = fetcherClass.getMethod("close");
        close.invoke(fetcher);
    }

    private static final class TestingFetchTaskContext implements FetchTask.Context {

        private final SourceConfig sourceConfig = new TestingSourceConfig();

        @Override
        public void configure(SourceSplitBase sourceSplitBase) {}

        @Override
        public ChangeEventQueue<DataChangeEvent> getQueue() {
            return null;
        }

        @Override
        public TableId getTableId(SourceRecord record) {
            return null;
        }

        @Override
        public Tables.TableFilter getTableFilter() {
            return null;
        }

        @Override
        public Offset getStreamOffset(SourceRecord record) {
            return null;
        }

        @Override
        public boolean isDataChangeRecord(SourceRecord record) {
            return false;
        }

        @Override
        public boolean isRecordBetween(
                SourceRecord record, Object[] splitStart, Object[] splitEnd) {
            return false;
        }

        @Override
        public void rewriteOutputBuffer(
                Map<Struct, SourceRecord> outputBuffer, SourceRecord changeRecord) {}

        @Override
        public List<SourceRecord> formatMessageTimestamp(Collection<SourceRecord> snapshotRecords) {
            return List.copyOf(snapshotRecords);
        }

        @Override
        public void close() {}

        @Override
        public DataSourceDialect getDataSourceDialect() {
            return null;
        }

        @Override
        public SourceConfig getSourceConfig() {
            return sourceConfig;
        }
    }

    private static final class TestingSourceConfig implements SourceConfig {

        @Override
        public StartupOptions getStartupOptions() {
            return StartupOptions.initial();
        }

        @Override
        public int getSplitSize() {
            return 0;
        }

        @Override
        public int getSplitMetaGroupSize() {
            return 0;
        }

        @Override
        public boolean isIncludeSchemaChanges() {
            return false;
        }

        @Override
        public boolean isCloseIdleReaders() {
            return false;
        }

        @Override
        public boolean isSkipSnapshotBackfill() {
            return false;
        }

        @Override
        public boolean isScanNewlyAddedTableEnabled() {
            return false;
        }

        @Override
        public boolean isAssignUnboundedChunkFirst() {
            return false;
        }
    }

    private static class Guava31BlockingClassLoader extends URLClassLoader {

        private Guava31BlockingClassLoader(URL[] urls, ClassLoader parent) {
            super(urls, parent);
        }

        @Override
        protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
            if (name.startsWith(SHADED_GUAVA31_PREFIX)) {
                throw new ClassNotFoundException(name);
            }
            if (name.endsWith("IncrementalSourceScanFetcher")
                    || name.endsWith("IncrementalSourceStreamFetcher")) {
                synchronized (getClassLoadingLock(name)) {
                    Class<?> loadedClass = findLoadedClass(name);
                    if (loadedClass == null) {
                        try {
                            loadedClass = findClass(name);
                        } catch (ClassNotFoundException ignored) {
                            loadedClass = super.loadClass(name, false);
                        }
                    }
                    if (resolve) {
                        resolveClass(loadedClass);
                    }
                    return loadedClass;
                }
            }
            return super.loadClass(name, resolve);
        }
    }
}
