/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.mysql.source.assigners;

import org.apache.flink.util.Preconditions;

/** The snapshot assigner state machine. */
public enum SnapshotAssignerStatus {
    /**
     * The assigner state machine goes this way.
     *
     * <pre>
     * INIT -> INIT_FINISH -> SUSPENDED -> RESUMED -> RESUMED_FINISH
     *                            ^                         |
     *                            |_________________________|
     * </pre>
     */
    INIT {
        @Override
        public int getValue() {
            return 0;
        }

        @Override
        public SnapshotAssignerStatus nextState() {
            return INIT_FINISH;
        }
    },
    INIT_FINISH {
        @Override
        public int getValue() {
            return 1;
        }

        @Override
        public SnapshotAssignerStatus nextState() {
            return SUSPENDED;
        }
    },
    SUSPENDED {
        @Override
        public int getValue() {
            return 2;
        }

        @Override
        public SnapshotAssignerStatus nextState() {
            return RESUMED;
        }
    },
    RESUMED {
        @Override
        public int getValue() {
            return 3;
        }

        @Override
        public SnapshotAssignerStatus nextState() {
            return RESUMED_FINISH;
        }
    },
    RESUMED_FINISH {
        @Override
        public int getValue() {
            return 4;
        }

        @Override
        public SnapshotAssignerStatus nextState() {
            return SUSPENDED;
        }
    };

    public abstract int getValue();

    public abstract SnapshotAssignerStatus nextState();

    public static SnapshotAssignerStatus fromInteger(int x) {
        Preconditions.checkState(x >= 0 && x < 5);
        switch (x) {
            case 0:
                return INIT;
            case 1:
                return INIT_FINISH;
            case 2:
                return SUSPENDED;
            case 3:
                return RESUMED;
            case 4:
                return RESUMED_FINISH;
        }
        return null;
    }
}
