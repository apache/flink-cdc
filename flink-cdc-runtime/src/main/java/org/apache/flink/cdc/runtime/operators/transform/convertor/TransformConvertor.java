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

package org.apache.flink.cdc.runtime.operators.transform.convertor;

import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.utils.StringUtils;
import org.apache.flink.cdc.runtime.operators.transform.PostTransformOperator;
import org.apache.flink.cdc.runtime.operators.transform.TransformRule;

import java.io.Serializable;
import java.util.Optional;

/**
 * The TransformConvertor applies to convert the {@link DataChangeEvent} after other part of {@link
 * TransformRule} in {@link PostTransformOperator}.
 */
public interface TransformConvertor extends Serializable {
    String SOFT_DELETE_CONVERTOR = "SOFT_DELETE";

    Optional<DataChangeEvent> convert(DataChangeEvent dataChangeEvent);

    static Optional<TransformConvertor> of(String classPath) {
        if (StringUtils.isNullOrWhitespaceOnly(classPath)) {
            return Optional.empty();
        }

        if (SOFT_DELETE_CONVERTOR.equals(classPath)) {
            return Optional.of(new SoftDeleteConvertor());
        }

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        try {
            Class<?> convertorClass = classLoader.loadClass(classPath);
            Object convertor = convertorClass.newInstance();
            if (convertor instanceof TransformConvertor) {
                return Optional.of((TransformConvertor) convertor);
            }
            throw new IllegalArgumentException(
                    String.format(
                            "%s is not an instance of %s",
                            classPath, TransformConvertor.class.getName()));
        } catch (Exception e) {
            throw new RuntimeException("Create transform convertor failed.", e);
        }
    }
}
