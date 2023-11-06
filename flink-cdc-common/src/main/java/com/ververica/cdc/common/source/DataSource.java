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

package com.ververica.cdc.common.source;

import org.apache.flink.annotation.PublicEvolving;

/**
 * {@code DataSource} is used to access metadata and read change data from external systems. It can
 * read data from multiple tables simultaneously.
 *
 * <p>The ability to perform full database synchronization comes from the implementation of the
 * Flink source. After obtaining user configuration, the Flink source connector needs to be able to
 * send all data from a specific database downstream according to the specified data type.
 */
@PublicEvolving
public interface DataSource {
    EventSourceProvider getEventSourceProvider();

    MetadataAccessor getMetadataAccessor();
}
