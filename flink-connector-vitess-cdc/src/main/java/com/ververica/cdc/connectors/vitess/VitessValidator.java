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

package com.ververica.cdc.connectors.vitess;

import org.apache.flink.shaded.guava30.com.google.common.collect.Maps;

import com.ververica.cdc.debezium.Validator;
import io.debezium.connector.vitess.VitessConnector;

import java.io.Serializable;
import java.util.Map;
import java.util.Properties;

/** The validator for Vitess. */
public class VitessValidator implements Validator, Serializable {

    private static final long serialVersionUID = 1L;

    private final Map<String, String> configuration;

    public VitessValidator(Properties properties) {
        this.configuration = Maps.fromProperties(properties);
    }

    @Override
    public void validate() {
        VitessConnector c = new VitessConnector();
        c.validate(configuration);
    }
}
