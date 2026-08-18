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

import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.udf.UserDefinedFunctionContext;
import org.apache.flink.cdc.python.utils.PemjaTestSupport;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link PythonUdf}. */
class PythonUdfTest {

    @BeforeAll
    static void requirePemja() {
        PemjaTestSupport.requirePemja();
    }

    @Test
    void evalBeforeOpenThrows() {
        PythonUdf udf = new PythonUdf();
        assertThatThrownBy(() -> udf.eval(1L))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("before open()");
    }

    @Test
    void closeIsIdempotent() throws Exception {
        PythonUdf udf = new PythonUdf();
        udf.close();
        udf.close();
        try {
            udf.open(contextFor("def eval(x: int) -> int:\n    return x\n"));
            udf.close();
            udf.close();
        } finally {
            udf.close();
        }
    }

    @Test
    void openRequiresSourceOption() {
        PythonUdf udf = new PythonUdf();
        Map<String, String> opts = new HashMap<>();
        opts.put(PythonUdf.OPTION_PYTHON_EXECUTABLE.key(), PemjaTestSupport.PYTHON_EXEC);
        assertThatThrownBy(() -> udf.open(() -> Configuration.fromMap(opts)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(PythonUdf.OPTION_SOURCE.key());
    }

    @Test
    void getReturnTypeReadsFromSource() {
        PythonUdf udf = new PythonUdf();
        assertThat(udf.getReturnType(contextFor("def eval(x: int) -> int:\n    return x * 2\n")))
                .isEqualTo(DataTypes.BIGINT());
    }

    @Test
    void evalRoundTripsInt() throws Exception {
        PythonUdf udf = new PythonUdf();
        udf.open(contextFor("def eval(x: int) -> int:\n    return x * 2\n"));
        try {
            assertThat(udf.eval(21L)).isEqualTo(42L);
        } finally {
            udf.close();
        }
    }

    @Test
    void evalRoundTripsString() throws Exception {
        PythonUdf udf = new PythonUdf();
        udf.open(contextFor("def eval(s: str) -> str:\n    return s.upper()\n"));
        try {
            assertThat(udf.eval("abc")).isEqualTo("ABC");
        } finally {
            udf.close();
        }
    }

    @Test
    void evalForwardsNullToPython() throws Exception {
        // Pemja maps Java null -> Python None; a guard-clause UDF should see it and may handle it.
        PythonUdf udf = new PythonUdf();
        udf.open(
                contextFor(
                        "def eval(s) -> str:\n" + "    return 'null' if s is None else str(s)\n"));
        try {
            assertThat(udf.eval(new Object[] {null})).isEqualTo("null");
        } finally {
            udf.close();
        }
    }

    @Test
    void evalImportsModuleFromDirectory(@TempDir Path tempDir) throws Exception {
        Path moduleDir = tempDir.resolve("python-dir");
        Files.createDirectories(moduleDir);
        Files.write(
                moduleDir.resolve("helper_mod.py"),
                ("def twice(x):\n" + "    return x * 2\n").getBytes(StandardCharsets.UTF_8));

        PythonUdf udf = new PythonUdf();
        udf.open(
                contextFor(
                        "import helper_mod\n"
                                + "def eval(x: int) -> int:\n"
                                + "    return helper_mod.twice(x)\n",
                        moduleDir.toString()));
        try {
            assertThat(udf.eval(21L)).isEqualTo(42L);
        } finally {
            udf.close();
        }
    }

    @Test
    void evalImportsModuleFromZip(@TempDir Path tempDir) throws Exception {
        Path zipFile = tempDir.resolve("python-deps.zip");
        try (ZipOutputStream zipOutputStream =
                new ZipOutputStream(Files.newOutputStream(zipFile))) {
            zipOutputStream.putNextEntry(new ZipEntry("helper_zip.py"));
            zipOutputStream.write(
                    ("def shout(s):\n" + "    return s.upper()\n")
                            .getBytes(StandardCharsets.UTF_8));
            zipOutputStream.closeEntry();
        }

        PythonUdf udf = new PythonUdf();
        udf.open(
                contextFor(
                        "import helper_zip\n"
                                + "def eval(s: str) -> str:\n"
                                + "    return helper_zip.shout(s)\n",
                        zipFile.toString()));
        try {
            assertThat(udf.eval("abc")).isEqualTo("ABC");
        } finally {
            udf.close();
        }
    }

    @Test
    void openFailureCleansUpInterpreterAndExtractedPythonFiles(@TempDir Path tempDir)
            throws Exception {
        Path zipFile = tempDir.resolve("python-deps.zip");
        try (ZipOutputStream zipOutputStream =
                new ZipOutputStream(Files.newOutputStream(zipFile))) {
            zipOutputStream.putNextEntry(new ZipEntry("helper.py"));
            zipOutputStream.write("VALUE = 1\n".getBytes(StandardCharsets.UTF_8));
            zipOutputStream.closeEntry();
        }

        Path extractedPathMarker = tempDir.resolve("extracted-path.txt");
        String markerPath =
                extractedPathMarker.toString().replace("\\", "\\\\").replace("'", "\\'");
        String source =
                "import sys\n"
                        + "from pathlib import Path\n"
                        + "extracted = next(p for p in sys.path if 'python-udf-files-' in p)\n"
                        + "Path('"
                        + markerPath
                        + "').write_text(extracted)\n"
                        + "raise RuntimeError('expected open failure')\n";

        PythonUdf udf = new PythonUdf();
        assertThatThrownBy(() -> udf.open(contextFor(source, zipFile.toString())))
                .hasMessageContaining("expected open failure");

        Path extractedPath = Paths.get(Files.readString(extractedPathMarker));
        assertThat(extractedPath).doesNotExist();
        assertThat(udf)
                .extracting("interpreter", "extractedPythonFilesDirectory")
                .containsExactly(null, null);
    }

    private static UserDefinedFunctionContext contextFor(String source) {
        return contextFor(source, null);
    }

    private static UserDefinedFunctionContext contextFor(String source, String pythonFiles) {
        Map<String, String> opts = new HashMap<>();
        opts.put(PythonUdf.OPTION_SOURCE.key(), source);
        opts.put(PythonUdf.OPTION_PYTHON_EXECUTABLE.key(), PemjaTestSupport.PYTHON_EXEC);
        if (pythonFiles != null) {
            opts.put(PythonUdf.OPTION_PYTHON_FILES.key(), pythonFiles);
        }
        return () -> Configuration.fromMap(opts);
    }
}
