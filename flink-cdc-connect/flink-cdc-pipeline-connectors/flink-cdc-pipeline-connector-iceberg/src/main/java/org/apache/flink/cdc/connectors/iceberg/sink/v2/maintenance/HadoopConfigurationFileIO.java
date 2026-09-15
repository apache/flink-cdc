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

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.io.DelegateFileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.FileInfo;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.SupportsBulkOperations;
import org.apache.iceberg.io.SupportsPrefixOperations;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.cdc.common.utils.Preconditions.checkArgument;

/** Preserves Hadoop configuration in Iceberg's JSON-serialized metadata scan tasks. */
@Internal
public class HadoopConfigurationFileIO implements FileIO {
    private static final long serialVersionUID = 1L;
    private static final String IMPLEMENTATION = "delegate-impl";
    private static final String PROPERTY_PREFIX = "delegate-property.";
    private static final String HADOOP_PREFIX = "hadoop-conf.";

    private FileIO delegate;
    private Map<String, String> properties;

    /** Required by Iceberg's FileIOParser when reconstructing a metadata scan task. */
    public HadoopConfigurationFileIO() {}

    static FileIO wrap(FileIO io) {
        if (!(io instanceof Configurable) || ((Configurable) io).getConf() == null) {
            return io;
        }
        Configuration conf = ((Configurable) io).getConf();
        Map<String, String> properties = new HashMap<>();
        properties.put(IMPLEMENTATION, io.getClass().getName());
        io.properties().forEach((key, value) -> properties.put(PROPERTY_PREFIX + key, value));
        conf.forEach(
                entry -> properties.put(HADOOP_PREFIX + entry.getKey(), conf.get(entry.getKey())));
        HadoopConfigurationFileIO result =
                io instanceof SupportsBulkOperations && io instanceof SupportsPrefixOperations
                        ? new WithOperations()
                        : new HadoopConfigurationFileIO();
        result.delegate = io;
        result.properties = Collections.unmodifiableMap(properties);
        return result;
    }

    @Override
    public void initialize(Map<String, String> properties) {
        String implementation = properties.get(IMPLEMENTATION);
        checkArgument(implementation != null, "Missing metadata FileIO implementation");
        checkArgument(
                !implementation.equals(HadoopConfigurationFileIO.class.getName())
                        && !implementation.equals(WithOperations.class.getName()),
                "Recursive metadata FileIO implementation");
        Map<String, String> delegateProperties = new HashMap<>();
        Configuration conf = new Configuration(false);
        properties.forEach(
                (key, value) -> {
                    if (key.startsWith(PROPERTY_PREFIX)) {
                        delegateProperties.put(key.substring(PROPERTY_PREFIX.length()), value);
                    } else if (key.startsWith(HADOOP_PREFIX)) {
                        conf.set(key.substring(HADOOP_PREFIX.length()), value);
                    }
                });
        delegate = CatalogUtil.loadFileIO(implementation, delegateProperties, conf);
        this.properties = Collections.unmodifiableMap(new HashMap<>(properties));
    }

    @Override
    public Map<String, String> properties() {
        return properties;
    }

    FileIO delegate() {
        return delegate;
    }

    @Override
    public InputFile newInputFile(String path) {
        return delegate.newInputFile(path);
    }

    @Override
    public InputFile newInputFile(String path, long length) {
        return delegate.newInputFile(path, length);
    }

    @Override
    public OutputFile newOutputFile(String path) {
        return delegate.newOutputFile(path);
    }

    @Override
    public void deleteFile(String path) {
        delegate.deleteFile(path);
    }

    @Override
    public void close() {
        if (delegate != null) {
            delegate.close();
        }
    }

    /** Retains the listing and deletion capabilities of the built-in Hadoop-backed FileIOs. */
    public static final class WithOperations extends HadoopConfigurationFileIO
            implements DelegateFileIO {
        private static final long serialVersionUID = 1L;

        public WithOperations() {}

        @Override
        public Iterable<FileInfo> listPrefix(String prefix) {
            return ((SupportsPrefixOperations) delegate()).listPrefix(prefix);
        }

        @Override
        public void deletePrefix(String prefix) {
            ((SupportsPrefixOperations) delegate()).deletePrefix(prefix);
        }

        @Override
        public void deleteFiles(Iterable<String> paths) {
            ((SupportsBulkOperations) delegate()).deleteFiles(paths);
        }
    }
}
