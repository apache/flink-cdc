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

package org.apache.flink.cdc.composer.flink.translator;

import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.composer.definition.RouteDef;
import org.apache.flink.cdc.runtime.operators.route.RouteFunction;
import org.apache.flink.cdc.runtime.typeutils.EventTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.util.List;

/** Translator used to build {@link RouteFunction}. */
public class RouteTranslator {

    public DataStream<Event> translate(DataStream<Event> input, List<RouteDef> routes) {
        if (routes.isEmpty()) {
            return input;
        }
        RouteFunction.Builder routeFunctionBuilder = RouteFunction.newBuilder();
        for (RouteDef route : routes) {
            routeFunctionBuilder.addRoute(
                    route.getSourceTable(), TableId.parse(route.getSinkTable()));
        }
        return input.map(routeFunctionBuilder.build(), new EventTypeInfo()).name("Route");
    }
}
