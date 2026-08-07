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

package org.apache.flink.cdc.python;

import org.apache.flink.cdc.common.annotation.Experimental;
import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.common.configuration.ConfigOptions;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.udf.UserDefinedFunction;
import org.apache.flink.cdc.common.udf.UserDefinedFunctionContext;
import org.apache.flink.cdc.python.utils.PythonUdfSignature;

import pemja.core.PythonInterpreter;
import pemja.core.PythonInterpreterConfig;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/** Generic UDF that delegates to a Python function defined inline in YAML. */
@Experimental
public final class PythonUdf implements UserDefinedFunction {

    public static final ConfigOption<String> OPTION_SOURCE =
            ConfigOptions.key("source")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Inline Python source containing a `def eval(...)`.");

    public static final ConfigOption<String> OPTION_PYTHON_EXECUTABLE =
            ConfigOptions.key("python-executable")
                    .stringType()
                    .defaultValue("python3")
                    .withDescription(
                            "Path to the Python interpreter Pemja embeds on every TaskManager."
                                    + " The interpreter must have a matching `pemja` package"
                                    + " installed; defaults to the first `python3` on PATH.");

    public static final ConfigOption<String> OPTION_PYTHON_FILES =
            ConfigOptions.key("python-files")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Comma-separated directories or zip archives that will be added"
                                    + " to the embedded Python import search path. Zip archives"
                                    + " are extracted to a temporary directory first so packages"
                                    + " with native extensions can be imported.");

    private static final String PYTHON_FUNCTION_NAME = "eval";

    private transient PythonInterpreter interpreter;
    private transient Path extractedPythonFilesDirectory;

    @Override
    public void open(UserDefinedFunctionContext context) {
        Configuration config = context.configuration();
        String source = requireSource(config);
        String pythonExec = config.get(OPTION_PYTHON_EXECUTABLE);

        PythonInterpreterConfig.PythonInterpreterConfigBuilder pemjaConfigBuilder =
                PythonInterpreterConfig.newBuilder().setPythonExec(pythonExec);
        configurePythonFiles(pemjaConfigBuilder, config);

        this.interpreter = new PythonInterpreter(pemjaConfigBuilder.build());
        this.interpreter.exec(source);
    }

    @Override
    public void close() {
        if (interpreter != null) {
            try {
                interpreter.close();
            } finally {
                interpreter = null;
            }
        }
        cleanupExtractedPythonFiles();
    }

    @Override
    public DataType getReturnType(UserDefinedFunctionContext context) {
        Configuration config = context.configuration();
        String source = requireSource(config);
        return PythonUdfSignature.parseReturnType(source, config.get(OPTION_PYTHON_EXECUTABLE));
    }

    public Object eval(Object... args) {
        if (interpreter == null) {
            throw new IllegalStateException("PythonUdf invoked before open() was called.");
        }
        return interpreter.invoke(PYTHON_FUNCTION_NAME, args);
    }

    private void configurePythonFiles(
            PythonInterpreterConfig.PythonInterpreterConfigBuilder pemjaConfigBuilder,
            Configuration config) {
        List<String> pythonPaths = new ArrayList<>();
        for (String rawPath : config.getOptional(OPTION_PYTHON_FILES).orElse("").split(",")) {
            String path = rawPath.trim();
            if (path.isEmpty()) {
                continue;
            }
            pythonPaths.add(resolvePythonFilePath(path));
        }
        if (!pythonPaths.isEmpty()) {
            pemjaConfigBuilder.addPythonPaths(String.join(File.pathSeparator, pythonPaths));
        }
    }

    private String resolvePythonFilePath(String configuredPath) {
        Path path = new File(configuredPath).toPath().toAbsolutePath().normalize();
        if (Files.isDirectory(path)) {
            return path.toString();
        }
        if (Files.isRegularFile(path)
                && path.getFileName().toString().toLowerCase(Locale.ROOT).endsWith(".zip")) {
            return extractPythonArchive(path).toString();
        }
        throw new IllegalArgumentException(
                "Python UDF option '"
                        + OPTION_PYTHON_FILES.key()
                        + "' only supports existing directories or .zip archives, but got: "
                        + configuredPath);
    }

    private Path extractPythonArchive(Path archivePath) {
        try {
            if (extractedPythonFilesDirectory == null) {
                extractedPythonFilesDirectory = Files.createTempDirectory("python-udf-files-");
            }
            String archiveName = archivePath.getFileName().toString();
            int suffixIndex = archiveName.toLowerCase(Locale.ROOT).lastIndexOf(".zip");
            String targetDirectoryName =
                    suffixIndex > 0 ? archiveName.substring(0, suffixIndex) : archiveName;
            Path targetDirectory =
                    Files.createTempDirectory(
                            extractedPythonFilesDirectory, targetDirectoryName + "-");
            unzipArchive(archivePath, targetDirectory);
            return targetDirectory;
        } catch (IOException e) {
            throw new IllegalArgumentException(
                    "Failed to extract Python dependency archive: " + archivePath, e);
        }
    }

    private static void unzipArchive(Path archivePath, Path targetDirectory) throws IOException {
        try (InputStream inputStream = Files.newInputStream(archivePath);
                ZipInputStream zipInputStream = new ZipInputStream(inputStream)) {
            ZipEntry zipEntry;
            while ((zipEntry = zipInputStream.getNextEntry()) != null) {
                Path targetPath = targetDirectory.resolve(zipEntry.getName()).normalize();
                if (!targetPath.startsWith(targetDirectory)) {
                    throw new IOException(
                            "Zip entry escapes extraction directory: " + zipEntry.getName());
                }
                if (zipEntry.isDirectory()) {
                    Files.createDirectories(targetPath);
                } else {
                    Path parent = targetPath.getParent();
                    if (parent != null) {
                        Files.createDirectories(parent);
                    }
                    Files.copy(zipInputStream, targetPath, StandardCopyOption.REPLACE_EXISTING);
                }
                zipInputStream.closeEntry();
            }
        }
    }

    private void cleanupExtractedPythonFiles() {
        if (extractedPythonFilesDirectory == null) {
            return;
        }
        try (Stream<Path> files = Files.walk(extractedPythonFilesDirectory)) {
            files.sorted(Comparator.reverseOrder())
                    .forEach(
                            path -> {
                                try {
                                    Files.deleteIfExists(path);
                                } catch (IOException ignored) {
                                    // Best-effort cleanup only.
                                }
                            });
        } catch (IOException ignored) {
            // Best-effort cleanup only.
        } finally {
            extractedPythonFilesDirectory = null;
        }
    }

    private static String requireSource(Configuration config) {
        return config.getOptional(OPTION_SOURCE)
                .orElseThrow(
                        () ->
                                new IllegalArgumentException(
                                        "Python UDF is missing required option '"
                                                + OPTION_SOURCE.key()
                                                + "'."));
    }
}
