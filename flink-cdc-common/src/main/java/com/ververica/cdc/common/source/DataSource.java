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

import com.ververica.cdc.common.annotation.PublicEvolving;

/**
 * {@code DataSource} is used to access metadata and read change data from external systems. It can
 * read data from multiple tables simultaneously.
 */
@PublicEvolving
public interface DataSource {

    /** Get the {@link EventSourceProvider} for reading events from external systems. */
    EventSourceProvider getEventSourceProvider();

    /** Get the {@link MetadataAccessor} for accessing metadata from external systems. */
    MetadataAccessor getMetadataAccessor();
}
