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

package org.apache.flink.core.execution;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.DescribedEnum;
import org.apache.flink.configuration.description.InlineElement;
import org.apache.flink.configuration.description.TextElement;

/**
 * Compatibility adapter for Flink 2.2. This class is part of the multi-version compatibility layer
 * that allows Flink CDC to work across different Flink versions.
 *
 * <p>Copy from <a
 * href="https://github.com/apache/flink/blob/release-1.20.3/flink-core/src/main/java/org/apache/flink/core/execution/RestoreMode.java">...</a>.
 */
@Internal
public enum RestoreMode implements DescribedEnum {
    CLAIM(
            "Flink will take ownership of the given snapshot. It will clean the snapshot once it is subsumed by newer ones."),
    NO_CLAIM(
            "Flink will not claim ownership of the snapshot files. However it will make sure it does not depend on any artefacts from the restored snapshot. In order to do that, Flink will take the first checkpoint as a full one, which means it might reupload/duplicate files that are part of the restored checkpoint."),
    /**
     * @deprecated
     */
    @Deprecated
    LEGACY(
            "This is the mode in which Flink worked until 1.15. It will not claim ownership of the snapshot and will not delete the files. However, it can directly depend on the existence of the files of the restored checkpoint. It might not be safe to delete checkpoints that were restored in legacy mode. This mode is deprecated, please use CLAIM or NO_CLAIM mode to get a clear state file ownership.");

    private final String description;
    public static final RestoreMode DEFAULT = NO_CLAIM;

    private RestoreMode(String description) {
        this.description = description;
    }

    @Internal
    public InlineElement getDescription() {
        return TextElement.text(this.description);
    }
}
