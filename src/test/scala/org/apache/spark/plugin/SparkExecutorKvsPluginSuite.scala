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
import org.apache.spark.plugin.grpc.KvsClient

class SparkExecutorKvsPluginSuite extends SparkFunSuite with LocalSparkContext {

  def resourcePath(f: String): String = {
    Thread.currentThread().getContextClassLoader.getResource(f).getPath
  }

  test("plugin initialization - standalone") {
    val conf = new SparkConf()
      .setAppName(getClass().getName())
      .set("spark.master", "local[1]")
      .set("spark.plugins", classOf[TestSparkExecutorKvsPlugin].getName())
      .set("spark.files", resourcePath("testdb.db"))

    sc = new SparkContext(conf)
    assert(TestSparkExecutorKvsPlugin.testSparkExecutorPlugin != null)

    val client = new KvsClient()
    assert(client.lookup("1") === "a")
    assert(client.lookup("2") === "b")
    assert(client.lookup("3") === "c")
    assert(client.lookup("4") === "")
  }

  test("plugin initialization - cluster") {
    val conf = new SparkConf()
      .setAppName(getClass().getName())
      .set(SparkLauncher.SPARK_MASTER, "local-cluster[1,4,1024]")
      .set("spark.plugins", classOf[TestSparkExecutorKvsPlugin].getName())
      .set("spark.files", resourcePath("testdb.db"))

    sc = new SparkContext(conf)
    assert(TestSparkExecutorKvsPlugin.testSparkExecutorPlugin != null)

    val rdd = sc.makeRDD(Seq("1", "2", "3", "4"), numSlices = 4)
    val resultRdd = rdd.map { key =>
      val client = new KvsClient()
      client.lookup(key)
    }
    assert(resultRdd.collect().toSet === Set("a", "b", "c", ""))
  }
}

object TestSparkExecutorKvsPlugin {
  var testSparkExecutorPlugin: ExecutorPlugin = _
}

class TestSparkExecutorKvsPlugin extends SparkExecutorKvsPlugin {
  override def executorPlugin(): ExecutorPlugin = {
    val plugin = super.executorPlugin()
    TestSparkExecutorKvsPlugin.testSparkExecutorPlugin = plugin
    plugin
  }
}