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

import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder

import java.util
import org.mapdb.{DBMaker, HTreeMap, Serializer}
import org.apache.spark.SparkFiles
import org.apache.spark.api.plugin.{DriverPlugin, ExecutorPlugin, PluginContext, SparkPlugin}
import org.apache.spark.plugin.grpc.AssociativeArrayServer

class SparkExecutorKvsPlugin extends SparkPlugin {
  // We don't use a driver-side plugin interface
  override def driverPlugin(): DriverPlugin = {
    new DriverPlugin {}
  }

  private def openAndInitMap(dbPath: String): String => String = {
    val dbPath = SparkFiles.get("testdb.db")
    val mapDb = DBMaker.fileDB(dbPath).fileMmapEnable().make()
    val map = mapDb
      .hashMap("map", Serializer.STRING, Serializer.STRING)
      .open()
      .asInstanceOf[HTreeMap[String, String]]

    (key: String) => map.get(key)
  }

  override def executorPlugin(): ExecutorPlugin = {
    new ExecutorPlugin() {
      var rpcServ: AssociativeArrayServer = _

      override def init(ctx: PluginContext, extraConf: util.Map[String, String]): Unit = {
        rpcServ = new AssociativeArrayServer(
          AssociativeArrayServer.DEFAULT_PORT, openAndInitMap("testdb.db"))
        super.init(ctx, extraConf)
      }

      override def shutdown(): Unit = {
        rpcServ.shutdown()
      }
    }
  }
}
