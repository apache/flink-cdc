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

package org.apache.flink.cdc.connectors.mysql.shade;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

/**
 * Pins the dep-vs-dep shade priority for the connector fat jar: the thin module is declared before
 * the connector dependency, while the generation common layer arrives transitively. On a same-FQCN
 * class conflict the thin module's class must win.
 *
 * <p>Skips itself on Flink lines whose compatibility layer carries no shade priority marker (the
 * marker classes exist on the 2.x line only).
 */
class ShadeOverridePriorityITCase {

    private static final String FAT_JAR_PREFIX = "flink-sql-connector-mysql-cdc-flink-";

    private static final String MARKER_ENTRY =
            "org/apache/flink/cdc/runtime/compat/ShadePriorityMarker.class";

    @Test
    void thinModuleMarkerWinsOverInheritedCommon() throws IOException {
        Path fatJar = findFatJar();
        try (ZipFile zip = new ZipFile(fatJar.toFile())) {
            ZipEntry markerEntry = zip.getEntry(MARKER_ENTRY);
            assumeThat(markerEntry)
                    .as("this Flink line carries no shade priority marker")
                    .isNotNull();
            byte[] markerBytes;
            try (InputStream in = zip.getInputStream(markerEntry)) {
                markerBytes = in.readAllBytes();
            }
            String markerText = new String(markerBytes, StandardCharsets.ISO_8859_1);
            assertThat(markerText).contains("-thin-module").doesNotContain("common");
        }
    }

    /** Locates the shaded fat jar without hardcoding the project version. */
    private static Path findFatJar() throws IOException {
        try (Stream<Path> files = Files.list(Paths.get("target"))) {
            List<Path> candidates =
                    files.filter(
                                    f -> {
                                        String name = f.getFileName().toString();
                                        return name.startsWith(FAT_JAR_PREFIX)
                                                && name.endsWith(".jar")
                                                && !name.startsWith("original-");
                                    })
                            .collect(Collectors.toList());
            assertThat(candidates).hasSize(1);
            return candidates.get(0);
        }
    }
}
