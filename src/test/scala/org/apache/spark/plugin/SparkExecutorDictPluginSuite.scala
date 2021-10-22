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
import java.util.Locale

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

import io.github.maropu.MapDbConverter

import org.apache.spark.{LocalSparkContext, SparkConf, SparkContext, SparkFunSuite}
import org.apache.spark.api.plugin.ExecutorPlugin
import org.apache.spark.launcher.SparkLauncher
import org.apache.spark.plugin.grpc.{BaseDictClient, DictClient, DictServer}

class SparkExecutorDictPluginSuite extends SparkFunSuite with LocalSparkContext {

  def resourcePath(f: String): String = {
    Thread.currentThread().getContextClassLoader.getResource(f).getPath
  }

  test("option - dbFile") {
    val key = "spark.plugins.executorDict.dbFile"
    val plugin = new SparkExecutorDictPlugin()
    val conf = new SparkConf()
    assert(plugin.dbPath(conf) === "")
    conf.set(key, "dbfile")
    assert(plugin.dbPath(conf) === "dbfile")
  }

  test("option - port") {
    val key = "spark.plugins.executorDict.port"
    val plugin = new SparkExecutorDictPlugin()
    val conf = new SparkConf()
    assert(plugin.port(conf) === "6543")
    conf.set(key, "6544")
    assert(plugin.port(conf) === "6544")
    val errMsg1 = intercept[RuntimeException] {
      plugin.port(conf.set(key, "xxx"))
    }.getMessage
    assert(errMsg1.contains("`spark.plugins.executorDict.port` " +
      "must be a positive number, but: xxx"))
    val errMsg2 = intercept[RuntimeException] {
      plugin.port(conf.set(key, "-1"))
    }.getMessage
    assert(errMsg2.contains("`spark.plugins.executorDict.port` " +
      "must be a positive number, but: -1"))
  }

  test("option - keyType") {
    val key = "spark.plugins.executorDict.keyType"
    val plugin = new SparkExecutorDictPlugin()
    val conf = new SparkConf()
    assert(plugin.keyType(conf) === "string")

    Seq("string", "STrInG", "int", "InT", "long", "LonG").foreach { tpeName =>
      conf.set(key, tpeName)
      assert(plugin.keyType(conf) === tpeName.toLowerCase(Locale.ROOT))
    }
    val errMsg = intercept[RuntimeException] {
      plugin.keyType(conf.set(key, "xxx"))
    }.getMessage
    assert(errMsg.contains("`spark.plugins.executorDict.keyType` " +
      "must be one of string/int/long, but: xxx"))
  }

  test("option - mapCacheSize") {
    val key = "spark.plugins.executorDict.mapCacheSize"
    val plugin = new SparkExecutorDictPlugin()
    val conf = new SparkConf()
    assert(plugin.mapCacheSize(conf) === "10000")
    conf.set(key, "300")
    assert(plugin.mapCacheSize(conf) === "300")
    val errMsg1 = intercept[RuntimeException] {
      plugin.mapCacheSize(conf.set(key, "xxx"))
    }.getMessage
    assert(errMsg1.contains("`spark.plugins.executorDict.mapCacheSize` " +
      "must be a positive number, but: xxx"))
    val errMsg2 = intercept[RuntimeException] {
      plugin.mapCacheSize(conf.set(key, "-1"))
    }.getMessage
    assert(errMsg2.contains("`spark.plugins.executorDict.mapCacheSize` " +
      "must be a positive number, but: -1"))
  }

  test("option - mapCacheConcurrencyLv") {
    val key = "spark.plugins.executorDict.mapCacheConcurrencyLv"
    val plugin = new SparkExecutorDictPlugin()
    val conf = new SparkConf()
    assert(plugin.mapCacheConcurrencyLv(conf) === "8")
    conf.set(key, "30")
    assert(plugin.mapCacheConcurrencyLv(conf) === "30")
    val errMsg1 = intercept[RuntimeException] {
      plugin.mapCacheConcurrencyLv(conf.set(key, "xxx"))
    }.getMessage
    assert(errMsg1.contains("`spark.plugins.executorDict.mapCacheConcurrencyLv` " +
      "must be a positive number, but: xxx"))
    val errMsg2 = intercept[RuntimeException] {
      plugin.mapCacheConcurrencyLv(conf.set(key, "-1"))
    }.getMessage
    assert(errMsg2.contains("`spark.plugins.executorDict.mapCacheConcurrencyLv` " +
      "must be a positive number, but: -1"))
  }

  test("option - mapKeyTypeCheckEnabled") {
    val key = "spark.plugins.executorDict.mapKeyTypeCheckEnabled"
    val plugin = new SparkExecutorDictPlugin()
    val conf = new SparkConf()
    assert(plugin.mapKeyTypeCheckEnabled(conf) === "true")
    conf.set(key, "false")
    assert(plugin.mapKeyTypeCheckEnabled(conf) === "false")
  }

  test("plugin initialization - standalone") {
    val conf = new SparkConf()
      .setAppName(getClass.getName)
      .set("spark.master", "local[1]")
      .set("spark.plugins", classOf[TestSparkExecutorDictPlugin].getName)
      .set("spark.files", resourcePath("string_key_dict.db"))

    sc = new SparkContext(conf)
    assert(TestSparkExecutorDictPlugin.testSparkExecutorPlugin != null)

    val client = DictClient[String]()
    assert(client.lookup("1") === "a")
    assert(client.lookup("2") === "b")
    assert(client.lookup("3") === "c")
    assert(client.lookup("4") === "")
  }

  test("plugin initialization - cluster") {
    val conf = new SparkConf()
      .setAppName(getClass.getName)
      .set(SparkLauncher.SPARK_MASTER, "local-cluster[1,4,1024]")
      .set("spark.plugins", classOf[TestSparkExecutorDictPlugin].getName)
      .set("spark.files", resourcePath("string_key_dict.db"))

    sc = new SparkContext(conf)
    assert(TestSparkExecutorDictPlugin.testSparkExecutorPlugin != null)

    val rdd = sc.makeRDD(Seq("1", "2", "3", "4"), numSlices = 4)
    val resultRdd = rdd.map { key =>
      val client = DictClient[String]()
      client.lookup(key)
    }
    assert(resultRdd.collect().toSet === Set("a", "b", "c", ""))
  }

  def testMultipleClients[K: ClassTag](testData: Map[K, String]): Unit = {
    var rpcServ: DictServer = null
    try {
      rpcServ = DictServer(6543, (key: K) => testData.getOrElse(key, ""), 10, 1, true)
      val threads = testData.map { case (k, expected) =>
        val t = new Thread() {
          override def run(): Unit = {
            val client = DictClient[K](6543)
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

  test("multiple client test - string keys") {
    testMultipleClients(Map("1" -> "a", "2" -> "b", "3" -> "c"))
  }

  test("multiple client test - int keys") {
    testMultipleClients(Map(1 -> "a", 2 -> "b", 3 -> "c"))
  }

  test("multiple client test - long keys") {
    testMultipleClients(Map(1L -> "a", 2L -> "b", 3L -> "c"))
  }

  test("errorOnRpcFailure in DictClient") {
    val keyValues = Seq("1" -> "a")
    val map = keyValues.toMap
    var rpcServ: DictServer = null
    var client1: BaseDictClient[String] = null
    var client2: BaseDictClient[String] = null
    try {
      rpcServ = DictServer(6543, (key: String) => map.getOrElse(key, ""), 10, 1, true)
      client1 = DictClient[String](6543, numRetries = 1, errorOnRpcFailure = false)
      client2 = DictClient[String](6543, numRetries = 1, errorOnRpcFailure = true)
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
      .setAppName(getClass.getName)
      .set(SparkLauncher.SPARK_MASTER, "local[1]")
      .set("spark.plugins", classOf[SparkExecutorDictPlugin].getName)
      .set("spark.files", resourcePath("invalid.db"))

    val errMsg = intercept[RuntimeException] {
      sc = new SparkContext(conf)
    }.getMessage
    assert(errMsg.contains("Cannot open a specified database"))
  }

  // scalastyle:off line.size.limit
  /**
   * TODO: Unexpected exception thrown like this:
   * java.io.FileNotFoundException: File file:/Users/maropu/Repositories/spark/spark-executor-dict-plugin/target/scala-2.12/test-classes/invalid.db} does not exist
   *   at org.apache.hadoop.fs.RawLocalFileSystem.deprecatedGetFileStatus(RawLocalFileSystem.java:611)
   *   at org.apache.hadoop.fs.RawLocalFileSystem.getFileLinkStatusInternal(RawLocalFileSystem.java:824)
   *   org.apache.hadoop.fs.RawLocalFileSystem.getFileStatus(RawLocalFileSystem.java:601)
   *   at org.apache.hadoop.fs.FilterFileSystem.getFileStatus(FilterFileSystem.java:428)
   *   ...
   */
  // scalastyle:on line.size.limit
  ignore("Multiple database files") {
    val files = s"${resourcePath("string_key_dict.db")},${resourcePath("invalid.db")}}"
    val conf = new SparkConf()
      .setAppName(getClass.getName)
      .set(SparkLauncher.SPARK_MASTER, "local[1]")
      .set("spark.plugins", classOf[SparkExecutorDictPlugin].getName)
      .set("spark.files", files)

    val errMsg = intercept[RuntimeException] {
      sc = new SparkContext(conf)
    }.getMessage
    assert(errMsg.contains("Multiple db files found: string_key_dict.db,invalid.db"))
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
      .setAppName(getClass.getName)
      .set("spark.master", "local[1]")
      .set("spark.plugins", classOf[SparkExecutorDictPlugin].getName)
      .set("spark.files", resourcePath("string_key_dict.db"))
      .set(DictPluginConf.EXECUTOR_DICT_PORT, s"$port")

    sc = new SparkContext(conf)

    val client = DictClient[String](port)
    assert(client.lookup("1") === "a")
    assert(client.lookup("2") === "b")
    assert(client.lookup("3") === "c")
    assert(client.lookup("4") === "")
  }

  test(s"config: ${DictPluginConf.EXECUTOR_DICT_DB_FILE}") {
    val conf = new SparkConf()
      .setAppName(getClass.getName)
      .set("spark.master", "local[1]")
      .set("spark.plugins", classOf[SparkExecutorDictPlugin].getName)
      .set(DictPluginConf.EXECUTOR_DICT_DB_FILE, resourcePath("string_key_dict.db"))

    sc = new SparkContext(conf)

    val client = DictClient[String]()
    assert(client.lookup("1") === "a")
    assert(client.lookup("2") === "b")
    assert(client.lookup("3") === "c")
    assert(client.lookup("4") === "")
  }

  ignore("large db file, sequential access") {
    withTempDir { tempDir =>
      val numKeys = 10000000
      val data = (0 until numKeys).map { i => s"$i" -> "abcdefghijklmn" }.toMap
      val dbPath = s"${tempDir.getAbsolutePath}/string_key_dict.db"
      MapDbConverter.save(dbPath, data)

      val dbSize = new File(dbPath).length
      val maxMem = Runtime.getRuntime.maxMemory
      assert(maxMem < dbSize)

      var rpcServ: DictServer = null
      try {
        val conf = Map("dbPath" -> dbPath, "port" -> "6543", "mapCacheSize" -> "1",
          "mapCacheConcurrencyLv" -> "8")
        rpcServ = SparkExecutorDictPlugin.initRpcServ(conf.asJava)
        val dict = DictClient[String]()
        (0 until 3).foreach { _ =>
          (0 until numKeys).foreach { i =>
            assert(dict.lookup(s"$i") === "abcdefghijklmn")
          }
        }
      } finally {
        if (rpcServ != null) {
          rpcServ.shutdown()
        }
      }
    }
  }

  test("unsupported types in client") {
    val errMsg = intercept[RuntimeException] {
      DictClient[Double]().lookup(1.0)
    }.getMessage
    assert(errMsg.contains("Unsupported Type: double"))
  }

  ignore("key type checks when opening db file") {
    val conf = new SparkConf()
      .setAppName(getClass.getName)
      .set("spark.master", "local[1]")
      .set("spark.plugins", classOf[TestSparkExecutorDictPlugin].getName)
      .set("spark.files", resourcePath("string_key_dict.db"))
      .set(DictPluginConf.EXECUTOR_DICT_MAP_KEY_TYPE, "int")

    // TODO: Why can this test pass?
    sc = new SparkContext(conf)
    assert(TestSparkExecutorDictPlugin.testSparkExecutorPlugin != null)
  }

  test("client key type checks enabled - string") {
    val conf = new SparkConf()
      .setAppName(getClass.getName)
      .set("spark.master", "local[1]")
      .set("spark.plugins", classOf[TestSparkExecutorDictPlugin].getName)
      .set("spark.files", resourcePath("string_key_dict.db"))
      .set(DictPluginConf.EXECUTOR_DICT_MAP_KEY_TYPE, "string")
      .set(DictPluginConf.EXECUTOR_DICT_MAP_KEY_TYPE_CHECK_ENABLED, "true")

    sc = new SparkContext(conf)
    assert(TestSparkExecutorDictPlugin.testSparkExecutorPlugin != null)
    assert(DictClient[String]().lookup("1") === "a")

    val errMsg1 = intercept[RuntimeException] {
      DictClient[Int]().lookup(1)
    }.getMessage
    assert(errMsg1.contains("RPC failed:"))

    val errMsg2 = intercept[RuntimeException] {
      DictClient[Long]().lookup(1)
    }.getMessage
    assert(errMsg2.contains("RPC failed:"))
  }

  test("client key type checks disabled - string") {
    val conf = new SparkConf()
      .setAppName(getClass.getName)
      .set("spark.master", "local[1]")
      .set("spark.plugins", classOf[TestSparkExecutorDictPlugin].getName)
      .set("spark.files", resourcePath("string_key_dict.db"))
      .set(DictPluginConf.EXECUTOR_DICT_MAP_KEY_TYPE, "string")
      .set(DictPluginConf.EXECUTOR_DICT_MAP_KEY_TYPE_CHECK_ENABLED, "false")

    sc = new SparkContext(conf)
    assert(TestSparkExecutorDictPlugin.testSparkExecutorPlugin != null)
    assert(DictClient[Int]().lookup(1) === "")
    assert(DictClient[Long]().lookup(1) === "")
  }

  test("client key type checks enabled - int") {
    val conf = new SparkConf()
      .setAppName(getClass.getName)
      .set("spark.master", "local[1]")
      .set("spark.plugins", classOf[TestSparkExecutorDictPlugin].getName)
      .set("spark.files", resourcePath("int_key_dict.db"))
      .set(DictPluginConf.EXECUTOR_DICT_MAP_KEY_TYPE, "int")
      .set(DictPluginConf.EXECUTOR_DICT_MAP_KEY_TYPE_CHECK_ENABLED, "true")

    sc = new SparkContext(conf)
    assert(TestSparkExecutorDictPlugin.testSparkExecutorPlugin != null)
    assert(DictClient[Int]().lookup(1) === "a")

    val errMsg1 = intercept[RuntimeException] {
      DictClient[String]().lookup("1")
    }.getMessage
    assert(errMsg1.contains("RPC failed:"))

    val errMsg2 = intercept[RuntimeException] {
      DictClient[Long]().lookup(1)
    }.getMessage
    assert(errMsg2.contains("RPC failed:"))
  }

  test("client key type checks disabled - int") {
    val conf = new SparkConf()
      .setAppName(getClass.getName)
      .set("spark.master", "local[1]")
      .set("spark.plugins", classOf[TestSparkExecutorDictPlugin].getName)
      .set("spark.files", resourcePath("int_key_dict.db"))
      .set(DictPluginConf.EXECUTOR_DICT_MAP_KEY_TYPE, "int")
      .set(DictPluginConf.EXECUTOR_DICT_MAP_KEY_TYPE_CHECK_ENABLED, "false")

    sc = new SparkContext(conf)
    assert(TestSparkExecutorDictPlugin.testSparkExecutorPlugin != null)
    assert(DictClient[String]().lookup("1") === "")
    assert(DictClient[Long]().lookup(1) === "")
  }
  test("client key type checks enabled - long") {
    val conf = new SparkConf()
      .setAppName(getClass.getName)
      .set("spark.master", "local[1]")
      .set("spark.plugins", classOf[TestSparkExecutorDictPlugin].getName)
      .set("spark.files", resourcePath("long_key_dict.db"))
      .set(DictPluginConf.EXECUTOR_DICT_MAP_KEY_TYPE, "long")
      .set(DictPluginConf.EXECUTOR_DICT_MAP_KEY_TYPE_CHECK_ENABLED, "true")

    sc = new SparkContext(conf)
    assert(TestSparkExecutorDictPlugin.testSparkExecutorPlugin != null)
    assert(DictClient[Long]().lookup(1) === "a")

    val errMsg1 = intercept[RuntimeException] {
      DictClient[String]().lookup("1")
    }.getMessage
    assert(errMsg1.contains("RPC failed:"))

    val errMsg2 = intercept[RuntimeException] {
      DictClient[Int]().lookup(1)
    }.getMessage
    assert(errMsg2.contains("RPC failed:"))
  }

  test("client key type checks disabled - long") {
    val conf = new SparkConf()
      .setAppName(getClass.getName)
      .set("spark.master", "local[1]")
      .set("spark.plugins", classOf[TestSparkExecutorDictPlugin].getName)
      .set("spark.files", resourcePath("long_key_dict.db"))
      .set(DictPluginConf.EXECUTOR_DICT_MAP_KEY_TYPE, "long")
      .set(DictPluginConf.EXECUTOR_DICT_MAP_KEY_TYPE_CHECK_ENABLED, "false")

    sc = new SparkContext(conf)
    assert(TestSparkExecutorDictPlugin.testSparkExecutorPlugin != null)
    assert(DictClient[String]().lookup("1") === "")
    assert(DictClient[Int]().lookup(1) === "")
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