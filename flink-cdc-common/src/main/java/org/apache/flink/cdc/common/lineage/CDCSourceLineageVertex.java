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

package org.apache.flink.cdc.common.lineage;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.streaming.api.lineage.LineageDataset;
import org.apache.flink.streaming.api.lineage.SourceLineageVertex;

import java.util.List;

/** A {@link SourceLineageVertex} implementation for CDC sources. */
public class CDCSourceLineageVertex implements SourceLineageVertex {

    private final Boundedness boundedness;
    private final List<LineageDataset> datasets;

    /**
     * Creates a CDC source lineage vertex.
     *
     * @param boundedness source boundedness
     * @param datasets datasets produced by the source
     */
    public CDCSourceLineageVertex(Boundedness boundedness, List<LineageDataset> datasets) {
        this.boundedness = boundedness;
        this.datasets = datasets;
    }

    /**
     * Returns whether the source is bounded or continuously unbounded.
     *
     * @return source boundedness
     */
    @Override
    public Boundedness boundedness() {
        return boundedness;
    }

    /**
     * Returns the datasets produced by the source.
     *
     * @return source lineage datasets
     */
    @Override
    public List<LineageDataset> datasets() {
        return datasets;
    }
}
