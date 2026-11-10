/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file distributed with
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

package org.apache.flink.cdc.flink;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies the maven-shade same-FQCN class priority: when the thin module and a dependency jar both
 * contribute a class with the same FQCN, the module's own class must win, while the remaining
 * dependency classes are still shaded in.
 */
class ShadeOverridePriorityITCase {

    private static final Path OWN_MARKER_CLASS =
            Paths.get(
                    "target",
                    "classes",
                    "org",
                    "apache",
                    "flink",
                    "cdc",
                    "runtime",
                    "compat",
                    "ShadePriorityMarker.class");
    private static final String MARKER_ENTRY =
            "org/apache/flink/cdc/runtime/compat/ShadePriorityMarker.class";

    @Test
    void moduleOwnClassWinsOverDependencyJarClass() throws IOException {
        Path shadePocJar = findShadePocJar();
        assertThat(OWN_MARKER_CLASS).exists();
        byte[] ownBytes = Files.readAllBytes(OWN_MARKER_CLASS);

        try (ZipFile zip = new ZipFile(shadePocJar.toFile())) {
            ZipEntry markerEntry = zip.getEntry(MARKER_ENTRY);
            assertThat(markerEntry).isNotNull();
            try (InputStream in = zip.getInputStream(markerEntry)) {
                assertThat(in.readAllBytes()).containsExactly(ownBytes);
            }
            assertThat(
                            zip.getEntry(
                                    "org/apache/flink/cdc/runtime/operators/AbstractStreamOperatorAdapter.class"))
                    .isNotNull();
            assertThat(zip.getEntry("org/apache/flink/cdc/source/RuntimeContextAdapter.class"))
                    .isNotNull();
        }
    }

    /** Locates the attached shade-poc artifact without hardcoding the project version. */
    private static Path findShadePocJar() throws IOException {
        try (Stream<Path> files = Files.list(Paths.get("target"))) {
            List<Path> candidates =
                    files.filter(f -> f.getFileName().toString().endsWith("-shade-poc.jar"))
                            .collect(Collectors.toList());
            assertThat(candidates).hasSize(1);
            return candidates.get(0);
        }
    }
}
