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

package org.apache.flink.cdc.common.transform.converter;

import org.apache.flink.cdc.common.annotation.Experimental;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.utils.StringUtils;

import java.io.Serializable;
import java.util.Optional;

/**
 * The PostTransformConverter applies to convert the {@link DataChangeEvent} after other part of
 * TransformRule in PostTransformOperator.
 */
@Experimental
public interface PostTransformConverter extends Serializable {

    Optional<DataChangeEvent> convert(DataChangeEvent dataChangeEvent);

    static Optional<PostTransformConverter> of(String classPath) {
        if (StringUtils.isNullOrWhitespaceOnly(classPath)) {
            return Optional.empty();
        }

        Optional<PostTransformConverter> postTransformConverter =
                PostTransformConverters.getInternalPostTransformConverter(classPath);
        if (postTransformConverter.isPresent()) {
            return postTransformConverter;
        }

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        try {
            Class<?> converterClass = classLoader.loadClass(classPath);
            Object converter = converterClass.newInstance();
            if (converter instanceof PostTransformConverter) {
                return Optional.of((PostTransformConverter) converter);
            }
            throw new IllegalArgumentException(
                    String.format(
                            "%s is not an instance of %s",
                            classPath, PostTransformConverter.class.getName()));
        } catch (Exception e) {
            throw new RuntimeException("Create post transform converter failed.", e);
        }
    }
}
