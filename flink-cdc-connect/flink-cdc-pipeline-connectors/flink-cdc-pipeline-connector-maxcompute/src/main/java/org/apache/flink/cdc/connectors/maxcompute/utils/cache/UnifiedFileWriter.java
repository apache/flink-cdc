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

package org.apache.flink.cdc.connectors.maxcompute.utils.cache;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.cdc.common.utils.Preconditions;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

/** UnifiedFileWriter. */
public class UnifiedFileWriter<T> {
    private static final long MAX_FILE_ROW_COUNT = 40960;
    private final String basePath;
    private int fileIndex = 0;
    private BufferedOutputStream currentOutputStream;
    private TypeSerializer<T> serializer;
    private Map<Integer, Long> fileRecordCountMap;

    public UnifiedFileWriter(String basePath, TypeSerializer<T> serializer) throws IOException {
        this.basePath = basePath;
        this.serializer = serializer;
        // make sure the directory exists
        Paths.get(basePath).toFile().mkdirs();
        currentOutputStream = createNewOutputStream();
        fileRecordCountMap = new HashMap<>();
    }

    public synchronized void write(T data) throws IOException {
        DataOutputView dov = new DataOutputViewStreamWrapper(currentOutputStream);
        serializer.serialize(data, dov);
        fileRecordCountMap.put(fileIndex, fileRecordCountMap.getOrDefault(fileIndex, 0L) + 1);

        if (fileRecordCountMap.get(fileIndex) > MAX_FILE_ROW_COUNT) {
            currentOutputStream.close();
            fileIndex++;
            currentOutputStream = createNewOutputStream();
        }
    }

    private BufferedOutputStream createNewOutputStream() throws IOException {
        String newFileName = basePath + "/data_" + fileIndex + ".cache";
        return new BufferedOutputStream(new FileOutputStream(newFileName, true));
    }

    public void close() throws IOException {
        currentOutputStream.close();
    }

    public void read(Function<T, Void> eventProcessor) throws IOException {
        Preconditions.checkNotNull(eventProcessor, "EventProcessor cannot be null.");
        File[] files = Paths.get(basePath).toFile().listFiles();
        if (files == null) {
            throw new IOException("Unable to list files from the directory");
        }
        Arrays.sort(files, Comparator.comparing(File::getName));

        // deal with each file in order
        for (int index = 0; index <= fileIndex; index++) {
            File file = files[index];
            try (InputStream fileInputStream = Files.newInputStream(file.toPath())) {
                DataInputView dataInputView = new DataInputViewStreamWrapper(fileInputStream);
                for (int i = 0; i < fileRecordCountMap.get(index); i++) {
                    T record = serializer.deserialize(dataInputView);
                    eventProcessor.apply(record);
                }
            }
            Files.delete(file.toPath());
        }
    }
}
