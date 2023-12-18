/*
 * Copyright 2023 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.common.factories;

import com.ververica.cdc.common.annotation.PublicEvolving;
import com.ververica.cdc.common.configuration.Configuration;

/** A helper for working with {@link Factory}. */
@PublicEvolving
public class FactoryHelper {

    /** Default implementation of {@link Factory.Context}. */
    public static class DefaultContext implements Factory.Context {

        private final Configuration factoryConfiguration;
        private final ClassLoader classLoader;
        private final Configuration pipelineConfiguration;

        public DefaultContext(
                Configuration factoryConfiguration,
                Configuration pipelineConfiguration,
                ClassLoader classLoader) {
            this.factoryConfiguration = factoryConfiguration;
            this.pipelineConfiguration = pipelineConfiguration;
            this.classLoader = classLoader;
        }

        @Override
        public Configuration getFactoryConfiguration() {
            return factoryConfiguration;
        }

        @Override
        public Configuration getPipelineConfiguration() {
            return pipelineConfiguration;
        }

        @Override
        public ClassLoader getClassLoader() {
            return classLoader;
        }
    }
}
