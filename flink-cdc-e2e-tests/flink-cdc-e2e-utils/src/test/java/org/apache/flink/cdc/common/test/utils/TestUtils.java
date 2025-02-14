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

package org.apache.flink.cdc.common.test.utils;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/** General test utilities. */
public class TestUtils {

    private static final ParameterProperty<Path> MODULE_DIRECTORY =
            new ParameterProperty<>("moduleDir", Paths::get);

    /**
     * Searches for a resource file matching the given regex in the given directory. This method is
     * primarily intended to be used for the initialization of static {@link Path} fields for
     * resource file(i.e. jar, config file). if resolvePaths is empty, this method will search file
     * under the modules {@code target} directory. if resolvePaths is not empty, this method will
     * search file under resolvePaths of current project.
     *
     * @param resourceNameRegex regex pattern to match against
     * @param resolvePaths an array of resolve paths of current project
     * @return Path pointing to the matching file
     * @throws RuntimeException if none or multiple resource files could be found
     */
    public static Path getResource(final String resourceNameRegex, String... resolvePaths) {
        // if the property is not set then we are most likely running in the IDE, where the working
        // directory is the
        // module of the test that is currently running, which is exactly what we want
        Path path = MODULE_DIRECTORY.get(Paths.get("").toAbsolutePath());
        if (resolvePaths != null && resolvePaths.length > 0) {
            path = path.getParent().getParent();
            for (String resolvePath : resolvePaths) {
                path = path.resolve(resolvePath);
            }
        }

        try (Stream<Path> dependencyResources = Files.walk(path)) {
            Optional<Path> jarPath =
                    dependencyResources
                            .filter(
                                    jar ->
                                            Pattern.compile(resourceNameRegex)
                                                    .matcher(jar.toAbsolutePath().toString())
                                                    .find())
                            .findFirst();
            if (jarPath.isPresent()) {
                return jarPath.get();
            } else {
                throw new RuntimeException(
                        new FileNotFoundException(
                                String.format(
                                        "No resource file could be found that matches the pattern %s. "
                                                + "This could mean that the test module must be rebuilt via maven.",
                                        resourceNameRegex)));
            }
        } catch (final IOException ioe) {
            throw new RuntimeException("Could not search for resource resource files.", ioe);
        }
    }
}
