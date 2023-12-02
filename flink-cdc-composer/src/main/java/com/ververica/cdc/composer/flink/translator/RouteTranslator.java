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

package com.ververica.cdc.composer.flink.translator;

import org.apache.flink.streaming.api.datastream.DataStream;

import com.ververica.cdc.common.event.Event;
import com.ververica.cdc.common.event.TableId;
import com.ververica.cdc.composer.definition.RouteDef;
import com.ververica.cdc.runtime.operators.route.RouteFunction;
import com.ververica.cdc.runtime.typeutils.EventTypeInfo;

import java.util.List;

/** Translator for router. */
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
