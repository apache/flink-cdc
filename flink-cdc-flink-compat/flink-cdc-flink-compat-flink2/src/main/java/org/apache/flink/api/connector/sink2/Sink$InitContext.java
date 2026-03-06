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

package org.apache.flink.api.connector.sink2;

/**
 * Compatibility stub for {@code Sink.InitContext} which was removed in Flink 2.x. Sink connector
 * classes compiled against Flink 1.20 still reference {@code Sink$InitContext} in their bytecode
 * (via {@code createWriter(Sink.InitContext)} method signatures). Java serialization introspects
 * all method parameter types via {@code getDeclaredMethods0()}, causing {@code
 * NoClassDefFoundError} on Flink 2.x if this class is absent. This empty stub makes the class
 * loadable without providing any actual implementation.
 */
// CHECKSTYLE.OFF: TypeName
public interface Sink$InitContext extends InitContext {}
// CHECKSTYLE.ON: TypeName
