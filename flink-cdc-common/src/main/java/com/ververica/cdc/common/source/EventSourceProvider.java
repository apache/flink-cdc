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

import org.apache.flink.api.connector.source.Source;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import com.ververica.cdc.common.annotation.PublicEvolving;

/**
 * A marker interface used to provide an event source for reading events from external systems. We
 * can reuse exiting Flink {@link Source} and Flink {@link SourceFunction} implementation, and we
 * can support our own {@code EventSource} implementation in the future.
 */
@PublicEvolving
public interface EventSourceProvider {}
