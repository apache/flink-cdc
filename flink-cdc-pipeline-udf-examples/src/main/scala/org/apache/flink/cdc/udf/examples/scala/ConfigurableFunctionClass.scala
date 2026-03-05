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

package org.apache.flink.cdc.udf.examples.scala

import org.apache.flink.cdc.common.configuration.ConfigOptions
import org.apache.flink.cdc.common.udf.{UserDefinedFunction, UserDefinedFunctionContext}

/** This is an example UDF class that reads options from configuration. */
class ConfigurableFunctionClass extends UserDefinedFunction {

  private var greeting: String = "Hello"
  private var suffix: String = "!"

  def eval(value: String): String = {
    greeting + " " + value + suffix
  }

  override def open(context: UserDefinedFunctionContext): Unit = {
    greeting = context.configuration().get(ConfigurableFunctionClass.GREETING)
    suffix = context.configuration().get(ConfigurableFunctionClass.SUFFIX)
  }

  override def close(): Unit = {}
}

object ConfigurableFunctionClass {
  private val GREETING = ConfigOptions.key("greeting").stringType().defaultValue("Hello")
  private val SUFFIX = ConfigOptions.key("suffix").stringType().defaultValue("!")
}
