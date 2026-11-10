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

package org.apache.flink.cdc.runtime.compat;

import org.apache.flink.annotation.Internal;

/**
 * Probe class for the maven-shade same-FQCN class priority: the flink-cdc-flink-2.2 thin module
 * carries a class with the same FQCN, and a shade run that sees both must keep the module's own
 * class, not the one from this dependency jar.
 */
@Internal
public class ShadePriorityMarker {

    /** Identifies the artifact this class was shaded from. */
    public static final String SOURCE = "flink2-common";

    private ShadePriorityMarker() {}
}
