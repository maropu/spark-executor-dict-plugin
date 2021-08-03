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

import java.io.File

import scala.collection.JavaConverters._
import scala.util.Random

import io.github.maropu.MapDbConverter

import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}
import org.apache.spark.plugin.grpc.{DictClient, DictServer}
import org.apache.spark.util.Utils

// scalastyle:off line.size.limit
/**
 * To run this benchmark:
 * {{{
 *   1. ./build/mvn scala:run -DmainClass=org.apache.spark.plugin.DictServiceBenchmark
 *   2. generate result:
 *     SPARK_GENERATE_BENCHMARK_FILES=1 ./build/mvn scala:run -DmainClass=org.apache.spark.plugin.DictServiceBenchmark
 *     Results will be written to "benchmarks/DictServiceBenchmark-results.txt".
 * }}}
 */
// scalastyle:on line.size.limit
object DictServiceBenchmark extends BenchmarkBase {

  private def test(
      numIters: Int,
      nThreads: Int = 1,
      refRatio: Double = 1.0,
      numKeys: Int,
      mapCacheSize: Int): Unit = {
    var rpcServ: DictServer = null
    val data = (0 until numKeys).map { i => s"$i" -> Random.nextInt(10000).toString }.toMap
    val tempDir = Utils.createTempDir()
    val dbPath = s"${tempDir.getAbsolutePath}/test.db"
    MapDbConverter.save(dbPath, data)
    try {
      val conf = Map("dbPath" -> dbPath, "port" -> "6543", "mapCacheSize" -> mapCacheSize.toString)
      rpcServ = SparkExecutorDictPlugin.initRpcServ(conf.asJava)
      val dbSize = new File(dbPath).length
      val benchmark = new Benchmark(
        s"lookup(n=$numIters,nThreads=$nThreads,dbSize=$dbSize,nKeys=$numKeys," +
          s"refRatio=$refRatio,nCache=$mapCacheSize)",
        numIters, output = output)
      benchmark.addCase(s"grpc.dict") { _: Int =>
        val dict = new DictClient(port = 6543)
        val n = numIters / nThreads
        val factor = (1.0 / refRatio).toInt
        val maxN = numKeys / factor
        val threads = (0 until nThreads).map { _ =>
          val t = new Thread() {
            override def run(): Unit = {
              var sum = 0L
              for (_ <- 0L until n) {
                val index = (Random.nextInt(maxN) * factor).toString
                sum += dict.lookup(index).toInt
              }
            }
          }
          t.start()
          t
        }
        threads.foreach(_.join())
      }
      benchmark.run()
    } finally {
      if (rpcServ != null) {
        rpcServ.shutdown()
      }
    }
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    test(numIters = 100000, numKeys = 100, mapCacheSize = 1)
    test(numIters = 100000, numKeys = 100, mapCacheSize = 100)
    test(numIters = 100000, nThreads = 1, numKeys = 3000000, mapCacheSize = 1)
    test(numIters = 100000, nThreads = 4, numKeys = 3000000, mapCacheSize = 1)
    test(numIters = 100000, refRatio = 0.0001, numKeys = 3000000, mapCacheSize = 1)
    test(numIters = 100000, refRatio = 0.0001, numKeys = 3000000, mapCacheSize = 1000)
  }
}
