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

import org.apache.flink.cdc.common.utils.Preconditions;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Comparator;
import java.util.function.Function;

/** UnifiedFileWriter. */
public class UnifiedFileWriter<T> {
    private static final long MAX_FILE_SIZE = 1024 * 1024 * 100;
    private final String basePath;
    private long currentFileSize = 0;
    private int fileIndex = 0;
    private BufferedOutputStream currentOutputStream;
    private SimpleVersionedSerializer<T> serializer;

    public UnifiedFileWriter(String basePath) throws IOException {
        this.basePath = basePath;
        // make sure the directory exists
        Paths.get(basePath).toFile().mkdirs();
        currentOutputStream = createNewOutputStream();
    }

    public synchronized void write(T data) throws IOException {
        byte[] serializedData = serializer.serialize(data);

        // write the data block length first
        byte[] sizePrefix = ByteBuffer.allocate(4).putInt(serializedData.length).array();

        if (currentFileSize + sizePrefix.length + serializedData.length > MAX_FILE_SIZE) {
            currentOutputStream.close();
            fileIndex++;
            currentOutputStream = createNewOutputStream();
            currentFileSize = 0;
        }

        // then write the data
        currentOutputStream.write(sizePrefix);
        currentOutputStream.write(serializedData);

        // update the current file size
        currentFileSize += sizePrefix.length + serializedData.length;
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
        for (File file : files) {
            try (InputStream input = new BufferedInputStream(Files.newInputStream(file.toPath()))) {
                // read until EOF
                while (true) {
                    // read the size prefix
                    byte[] sizePrefix = new byte[4];
                    if (input.read(sizePrefix) != sizePrefix.length) {
                        break;
                    }
                    int size = ByteBuffer.wrap(sizePrefix).getInt();
                    // read the data
                    byte[] data = new byte[size];
                    if (input.read(data) != size) {
                        throw new EOFException("Unexpected end of file while reading data block.");
                    }
                    T record = serializer.deserialize(serializer.getVersion(), data);
                    // process the record
                    eventProcessor.apply(record);
                }
            }
            Files.delete(file.toPath());
        }
    }
}
