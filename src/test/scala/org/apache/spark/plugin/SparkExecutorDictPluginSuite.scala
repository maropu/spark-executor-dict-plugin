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

import org.apache.spark.{LocalSparkContext, SparkConf, SparkContext, SparkFunSuite}
import org.apache.spark.api.plugin.ExecutorPlugin
import org.apache.spark.launcher.SparkLauncher
import org.apache.spark.plugin.grpc.{DictClient, DictServer}

class SparkExecutorDictPluginSuite extends SparkFunSuite with LocalSparkContext {

  def resourcePath(f: String): String = {
    Thread.currentThread().getContextClassLoader.getResource(f).getPath
  }

  test("plugin initialization - standalone") {
    val conf = new SparkConf()
      .setAppName(getClass().getName())
      .set("spark.master", "local[1]")
      .set("spark.plugins", classOf[TestSparkExecutorDictPlugin].getName())
      .set("spark.files", resourcePath("test.db"))

    sc = new SparkContext(conf)
    assert(TestSparkExecutorDictPlugin.testSparkExecutorPlugin != null)

    val client = new DictClient()
    assert(client.lookup("1") === "a")
    assert(client.lookup("2") === "b")
    assert(client.lookup("3") === "c")
    assert(client.lookup("4") === "")
  }

  test("plugin initialization - cluster") {
    val conf = new SparkConf()
      .setAppName(getClass().getName())
      .set(SparkLauncher.SPARK_MASTER, "local-cluster[1,4,1024]")
      .set("spark.plugins", classOf[TestSparkExecutorDictPlugin].getName())
      .set("spark.files", resourcePath("test.db"))

    sc = new SparkContext(conf)
    assert(TestSparkExecutorDictPlugin.testSparkExecutorPlugin != null)

    val rdd = sc.makeRDD(Seq("1", "2", "3", "4"), numSlices = 4)
    val resultRdd = rdd.map { key =>
      val client = new DictClient()
      client.lookup(key)
    }
    assert(resultRdd.collect().toSet === Set("a", "b", "c", ""))
  }

  test("multiple client test") {
    val keyValues = Seq("1" -> "a", "2" -> "b", "3" -> "c")
    val map = keyValues.toMap
    var rpcServ: DictServer = null
    try {
      rpcServ = new DictServer(6543, (key: String) => map.getOrElse(key, ""), 10, 1)
      val threads = keyValues.map { case (k, expected) =>
        val t = new Thread() {
          override def run(): Unit = {
            val client = new DictClient(6543)
            (0 until 1000).foreach { _ =>
              assert(client.lookup(k) === expected)
            }
          }
        }
        t.start()
        t
      }
      threads.foreach(_.join())
    } finally {
      if (rpcServ != null) {
        rpcServ.shutdown()
      }
    }
  }

  test("errorOnRpcFailure in DictClient") {
    val keyValues = Seq("1" -> "a")
    val map = keyValues.toMap
    var rpcServ: DictServer = null
    var client1: DictClient = null
    var client2: DictClient = null
    try {
      rpcServ = new DictServer(6543, (key: String) => map.getOrElse(key, ""), 10, 1)
      client1 = new DictClient(6543, numRetries = 1, errorOnRpcFailure = false)
      client2 = new DictClient(6543, numRetries = 1, errorOnRpcFailure = true)
      rpcServ.shutdown()

      assert(client1.lookup("1") === "")

      val errMsg = intercept[RuntimeException] {
        client2.lookup("1")
      }.getMessage
      assert(errMsg.contains("RPC failed:"))
    } finally {
      if (rpcServ != null) {
        rpcServ.shutdown()
      }
    }
  }

  test("Invalid database file") {
    val conf = new SparkConf()
      .setAppName(getClass().getName())
      .set(SparkLauncher.SPARK_MASTER, "local[1]")
      .set("spark.plugins", classOf[SparkExecutorDictPlugin].getName())
      .set("spark.files", resourcePath("invalid.db"))

    val errMsg = intercept[RuntimeException] {
      sc = new SparkContext(conf)
    }.getMessage
    assert(errMsg.contains("Cannot open a specified database"))
  }

  ignore("Multiple database files") {
    val files = s"${resourcePath("test.db")},${resourcePath("invalid.db")}}"
    val conf = new SparkConf()
      .setAppName(getClass().getName())
      .set(SparkLauncher.SPARK_MASTER, "local[1]")
      .set("spark.plugins", classOf[SparkExecutorDictPlugin].getName())
      .set("spark.files", files)

    val errMsg = intercept[RuntimeException] {
      sc = new SparkContext(conf)
    }.getMessage
    assert(errMsg.contains("Multiple db files found: test.db,invalid.db"))
  }

  test("No database file") {
    val conf = new SparkConf()
      .setAppName(getClass().getName())
      .set(SparkLauncher.SPARK_MASTER, "local[1]")
      .set("spark.plugins", classOf[SparkExecutorDictPlugin].getName())

    val errMsg = intercept[RuntimeException] {
      sc = new SparkContext(conf)
    }.getMessage
    assert(errMsg.contains("No db file found"))
  }

  test(s"config: ${DictPluginConf.EXECUTOR_DICT_PORT}") {
    val port = 7654
    val conf = new SparkConf()
      .setAppName(getClass().getName())
      .set("spark.master", "local[1]")
      .set("spark.plugins", classOf[SparkExecutorDictPlugin].getName())
      .set("spark.files", resourcePath("test.db"))
      .set(DictPluginConf.EXECUTOR_DICT_PORT, s"$port")

    sc = new SparkContext(conf)

    val client = new DictClient(port)
    assert(client.lookup("1") === "a")
    assert(client.lookup("2") === "b")
    assert(client.lookup("3") === "c")
    assert(client.lookup("4") === "")
  }

  test(s"config: ${DictPluginConf.EXECUTOR_DICT_DB_FILE}") {
    val conf = new SparkConf()
      .setAppName(getClass().getName())
      .set("spark.master", "local[1]")
      .set("spark.plugins", classOf[SparkExecutorDictPlugin].getName())
      .set(DictPluginConf.EXECUTOR_DICT_DB_FILE, resourcePath("test.db"))

    sc = new SparkContext(conf)

    val client = new DictClient()
    assert(client.lookup("1") === "a")
    assert(client.lookup("2") === "b")
    assert(client.lookup("3") === "c")
    assert(client.lookup("4") === "")
  }
}

object TestSparkExecutorDictPlugin {
  var testSparkExecutorPlugin: ExecutorPlugin = _
}

class TestSparkExecutorDictPlugin extends SparkExecutorDictPlugin {
  override def executorPlugin(): ExecutorPlugin = {
    val plugin = super.executorPlugin()
    TestSparkExecutorDictPlugin.testSparkExecutorPlugin = plugin
    plugin
  }
}