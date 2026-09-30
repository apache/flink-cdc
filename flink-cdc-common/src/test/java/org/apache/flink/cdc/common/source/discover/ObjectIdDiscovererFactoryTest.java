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

package org.apache.flink.cdc.common.source.discover;

import org.apache.flink.cdc.common.event.TableId;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link ObjectIdDiscovererFactory#discoverFactory}. */
class ObjectIdDiscovererFactoryTest {

    @Test
    void testMatchesByTypeAndObjectIdClass() {
        ObjectIdDiscovererFactory<String> factory =
                ObjectIdDiscovererFactory.discoverFactory(
                        "jdbc",
                        String.class,
                        Arrays.asList(new TestTableFactory(), new TestStringFactory()));

        assertThat(factory).isInstanceOf(TestStringFactory.class);
    }

    @Test
    void testFailsForUnknownCombination() {
        assertThatThrownBy(
                        () ->
                                ObjectIdDiscovererFactory.discoverFactory(
                                        "jdbc",
                                        Long.class,
                                        Arrays.asList(
                                                new TestTableFactory(), new TestStringFactory())))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("jdbc")
                .hasMessageContaining(Long.class.getName())
                .hasMessageContaining(TableId.class.getName())
                .hasMessageContaining(String.class.getName());
    }

    @Test
    void testFailsForDuplicateCombination() {
        assertThatThrownBy(
                        () ->
                                ObjectIdDiscovererFactory.discoverFactory(
                                        "jdbc",
                                        TableId.class,
                                        Arrays.asList(
                                                new TestTableFactory(),
                                                new DuplicateTestTableFactory())))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(TestTableFactory.class.getName())
                .hasMessageContaining(DuplicateTestTableFactory.class.getName());
    }

    private static final class TestTableFactory implements ObjectIdDiscovererFactory<TableId> {

        @Override
        public String type() {
            return "jdbc";
        }

        @Override
        public Class<TableId> objectIdClass() {
            return TableId.class;
        }

        @Override
        public ObjectIdDiscoverer<TableId> createDiscoverer() {
            return new EmptyDiscoverer<>();
        }
    }

    private static final class DuplicateTestTableFactory
            implements ObjectIdDiscovererFactory<TableId> {

        @Override
        public String type() {
            return "jdbc";
        }

        @Override
        public Class<TableId> objectIdClass() {
            return TableId.class;
        }

        @Override
        public ObjectIdDiscoverer<TableId> createDiscoverer() {
            return new EmptyDiscoverer<>();
        }
    }

    private static final class TestStringFactory implements ObjectIdDiscovererFactory<String> {

        @Override
        public String type() {
            return "jdbc";
        }

        @Override
        public Class<String> objectIdClass() {
            return String.class;
        }

        @Override
        public ObjectIdDiscoverer<String> createDiscoverer() {
            return new EmptyDiscoverer<>();
        }
    }

    private static final class EmptyDiscoverer<T> implements ObjectIdDiscoverer<T> {

        @Override
        public void open(Context context) {}

        @Override
        public Set<T> discover() {
            return Collections.emptySet();
        }

        @Override
        public void close() {}
    }
}
