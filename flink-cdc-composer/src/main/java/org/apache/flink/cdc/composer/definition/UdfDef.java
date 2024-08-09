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

package org.apache.flink.cdc.composer.definition;

import java.util.Objects;

/**
 * Definition of a user-defined function.
 *
 * <p>A transformation definition contains:
 *
 * <ul>
 *   <li>name: Static method name of user-defined functions.
 *   <li>classpath: Fully-qualified class path of package containing given function.
 * </ul>
 */
public class UdfDef {
    private final String name;
    private final String classpath;

    public UdfDef(String name, String classpath) {
        this.name = name;
        this.classpath = classpath;
    }

    public String getName() {
        return name;
    }

    public String getClasspath() {
        return classpath;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        UdfDef udfDef = (UdfDef) o;
        return Objects.equals(name, udfDef.name) && Objects.equals(classpath, udfDef.classpath);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, classpath);
    }

    @Override
    public String toString() {
        return "UdfDef{" + "name='" + name + '\'' + ", classpath='" + classpath + '\'' + '}';
    }
}
