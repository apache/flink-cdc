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

package org.apache.flink.table.factories;

/**
 * Minimal compatibility stub for Flink's CatalogFactory which existed in older Flink table APIs and
 * is still referenced by some connector artifacts (for example Paimon compiled against Flink 1.x).
 * This interface is intentionally empty and only restores the missing type so that such connectors
 * can be loaded on Flink 2.2 runtimes.
 */
// CHECKSTYLE.OFF: TypeName
public interface CatalogFactory {}
// CHECKSTYLE.ON: TypeName
