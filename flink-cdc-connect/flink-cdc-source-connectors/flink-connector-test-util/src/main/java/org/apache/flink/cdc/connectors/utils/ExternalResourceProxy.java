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

package org.apache.flink.cdc.connectors.utils;

import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.rules.ExternalResource;

import java.lang.reflect.Method;

/** A wrapper class for migrating JUnit 4 {@code @Rule} to JUnit 5. */
public class ExternalResourceProxy<T extends ExternalResource>
        implements BeforeEachCallback, AfterEachCallback {

    private final Class<?> resourceClazz;
    private final T resource;

    public ExternalResourceProxy(T resource) {
        this.resourceClazz = resource.getClass();
        this.resource = resource;
    }

    public T get() {
        return resource;
    }

    @Override
    public void beforeEach(ExtensionContext extensionContext) throws Exception {
        invoke(resourceClazz, resource, "before");
    }

    @Override
    public void afterEach(ExtensionContext extensionContext) throws Exception {
        invoke(resourceClazz, resource, "after");
    }

    private void invoke(Class<?> clazz, Object object, String methodName) throws Exception {
        Method method = clazz.getDeclaredMethod(methodName);
        method.setAccessible(true);
        method.invoke(object);
    }
}
