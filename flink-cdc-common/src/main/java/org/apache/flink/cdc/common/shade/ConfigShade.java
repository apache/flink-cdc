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

package org.apache.flink.cdc.common.shade;

import org.apache.flink.cdc.common.annotation.PublicEvolving;
import org.apache.flink.cdc.common.configuration.Configuration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The interface that provides the ability to decrypt {@link
 * org.apache.flink.cdc.composer.definition}.
 */
@PublicEvolving
public interface ConfigShade {
    Logger LOG = LoggerFactory.getLogger(ConfigShade.class);

    /**
     * Initializes the custom instance using the pipeline configuration.
     *
     * <p>This method can be useful when decryption requires an external file (e.g. a key file)
     * defined in the pipeline configs.
     */
    default void initialize(Configuration pipelineConfig) throws Exception {}

    /**
     * The unique identifier of the current interface, used it to select the correct {@link
     * ConfigShade}.
     */
    String getIdentifier();

    /**
     * Decrypt the content.
     *
     * @param content The content to decrypt
     */
    String decrypt(String content);
}
