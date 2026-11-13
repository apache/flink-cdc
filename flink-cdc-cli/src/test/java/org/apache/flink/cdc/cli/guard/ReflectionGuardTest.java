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

package org.apache.flink.cdc.cli.guard;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

/**
 * Mechanical guard for the adapter rule: main code (outside flink-cdc-flink) must not reflect into
 * {@code org.apache.flink.*} internals. Scans the compiled classes of the core modules for classes
 * whose constant pool contains both reflection-API references (setAccessible / getDeclaredMethod /
 * getDeclaredField / getDeclaredConstructor) and {@code org/apache/flink/} type descriptors, and
 * asserts the violating set equals the whitelist. Each violation covered by a compatibility adapter
 * is removed from the whitelist; the guard then fails until the reflection is gone.
 *
 * <p>Known boundary: reflective class instantiation without the scanned markers (e.g. bare {@code
 * Class.forName(...).newInstance()}) is not detected; this heuristic scan may later be replaced by
 * an ArchUnit-based guard.
 */
class ReflectionGuardTest {

    /**
     * Legacy reflection sites still present; shrinks to empty as the remaining reflection sites
     * converge Keeping a class here past its convergence keeps the guard red.
     */
    private static final Set<String> VIOLATION_WHITELIST = Set.of();

    /**
     * Classes whose reflective access targets are NOT {@code org.apache.flink.*} (e.g. UDF
     * instantiation by user-supplied class name). The byte-scan heuristic cannot tell the
     * reflection target apart, so these confirmed non-Flink reflection sites are exempt.
     */
    private static final Set<String> REFLECTION_EXEMPTIONS =
            Set.of(
                    "org.apache.flink.cdc.runtime.operators.transform.async.AsyncPostTransformFunction",
                    "org.apache.flink.cdc.runtime.operators.transform.PostTransformOperator",
                    "org.apache.flink.cdc.runtime.operators.transform.TransformExpressionCompiler",
                    "org.apache.flink.cdc.runtime.operators.transform.UserDefinedFunctionDescriptor",
                    "org.apache.flink.cdc.runtime.parser.TransformParser");

    @Test
    void mainCodeMustNotReflectIntoFlinkInternals() throws IOException {
        List<String> violations = new ArrayList<>();
        for (Path classesRoot : classesRoots()) {
            assumeThat(Files.isDirectory(classesRoot))
                    .as("Module not built yet: %s", classesRoot)
                    .isTrue();
            try (Stream<Path> files = Files.walk(classesRoot)) {
                files.filter(f -> f.toString().endsWith(".class"))
                        .forEach(f -> collect(f, classesRoot, violations));
            }
        }
        System.out.println("Reflection guard violations: " + violations);
        assertThat(violations)
                .as("Main code classes still reflecting into org.apache.flink internals")
                .containsExactlyInAnyOrderElementsOf(VIOLATION_WHITELIST);
    }

    private static void collect(Path classFile, Path root, List<String> violations) {
        if (reflectsIntoFlink(classFile)
                && !REFLECTION_EXEMPTIONS.contains(toClassName(root, classFile))) {
            violations.add(toClassName(root, classFile));
        }
    }

    private static List<Path> classesRoots() {
        return List.of(
                Paths.get("..", "flink-cdc-common", "target", "classes"),
                Paths.get("..", "flink-cdc-runtime", "target", "classes"),
                Paths.get("..", "flink-cdc-composer", "target", "classes"),
                Paths.get("target", "classes"));
    }

    private static boolean reflectsIntoFlink(Path classFile) {
        String bytes;
        try {
            bytes = new String(Files.readAllBytes(classFile), StandardCharsets.ISO_8859_1);
        } catch (IOException e) {
            throw new RuntimeException("Failed to read " + classFile, e);
        }
        boolean referencesFlink = bytes.contains("org/apache/flink/");
        boolean usesReflectionApi = bytes.contains("java/lang/reflect/");
        boolean setsAccessibleOrDeclaresMember =
                bytes.contains("setAccessible")
                        || bytes.contains("getDeclaredMethod")
                        || bytes.contains("getDeclaredField")
                        || bytes.contains("getDeclaredConstructor");
        boolean reflectApiViolation =
                usesReflectionApi && setsAccessibleOrDeclaresMember && referencesFlink;
        // Class.forName(String) method reference: the constant pool stores the signature as a
        // single UTF8 constant, unlike the split name/descriptor entries of a Methodref.
        boolean classForNameViolation =
                bytes.contains("(Ljava/lang/String;)Ljava/lang/Class;") && referencesFlink;
        return reflectApiViolation || classForNameViolation;
    }

    private static String toClassName(Path root, Path classFile) {
        String relative = root.relativize(classFile).toString();
        return relative.substring(0, relative.length() - ".class".length())
                .replace('\\', '.')
                .replace('/', '.');
    }
}
