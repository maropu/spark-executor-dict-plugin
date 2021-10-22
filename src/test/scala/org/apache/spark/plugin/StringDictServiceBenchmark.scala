/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.plugin

// scalastyle:off line.size.limit
/**
 * To run this benchmark:
 * {{{
 *   1. ./build/mvn scala:run -DmainClass=org.apache.spark.plugin.StringDictServiceBenchmark
 *   2. generate result:
 *     SPARK_GENERATE_BENCHMARK_FILES=1 ./build/mvn scala:run -DmainClass=org.apache.spark.plugin.StringDictServiceBenchmark
 *     Results will be written to "benchmarks/StringDictServiceBenchmark-results.txt".
 * }}}
 */
// scalastyle:on line.size.limit
object StringDictServiceBenchmark extends BaseDictServiceBenchmark[String] {

  def keyType: String = "string"
  def castTo(v: Int): String = v.toString
}
