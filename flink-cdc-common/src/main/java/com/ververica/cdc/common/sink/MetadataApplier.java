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

package com.ververica.cdc.common.sink;

import com.ververica.cdc.common.annotation.PublicEvolving;
import com.ververica.cdc.common.event.SchemaChangeEvent;

import java.io.Serializable;

/** {@code MetadataApplier} is used to apply metadata changes to external systems. */
@PublicEvolving
public interface MetadataApplier extends Serializable {

    /** Apply the given {@link SchemaChangeEvent} to external systems. */
    void applySchemaChange(SchemaChangeEvent schemaChangeEvent);
}
