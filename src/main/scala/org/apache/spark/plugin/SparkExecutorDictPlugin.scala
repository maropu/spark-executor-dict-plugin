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
import java.net.{InetAddress, ServerSocket}
import java.util

import scala.reflect.{classTag, ClassTag}
import scala.util.Try
import scala.util.control.NonFatal

import org.mapdb.{DBMaker, Serializer}

import org.apache.spark.{SparkConf, SparkContext, SparkEnv, SparkFiles}
import org.apache.spark.api.plugin.{DriverPlugin, ExecutorPlugin, PluginContext, SparkPlugin}
import org.apache.spark.internal.Logging
import org.apache.spark.plugin.grpc.DictServer
import org.apache.spark.util.Utils

object SparkExecutorDictPlugin extends Logging {

  val DEFAULT_PORT = "6543"

  private def getDbPathFromSparkFiles(): String = {
    val files = new File(SparkFiles.getRootDirectory())
    val dbFile = files.listFiles.filter(_.getName.endsWith(".db"))
    if (dbFile.length == 0) {
      throw new RuntimeException("No db file found")
    } else if (dbFile.length > 1) {
      throw new RuntimeException(
        s"Multiple db files found: ${dbFile.map(_.getName).mkString(",")}")
    }
    dbFile.head.getAbsolutePath
  }

  private def getKeySerializer[K: ClassTag]() = classTag[K].runtimeClass match {
    case c: Class[_] if c == classOf[String] => Serializer.STRING
    case c: Class[_] if c == java.lang.Integer.TYPE => Serializer.INTEGER
    case c: Class[_] if c == java.lang.Long.TYPE => Serializer.LONG
    case c => throw new IllegalStateException(s"Unsupported type: ${c.getSimpleName}")
  }

  private def openMapDb[K: ClassTag](dbPath: String) = {
    // Since MapDB `hashMap` uses a tree structure internally, the data reads
    // can be slower than those of a memory-based hash map. So, we should
    // cache frequently-accessed items for fast lookup.
    // - https://jankotek.gitbooks.io/mapdb/content/htreemap/
    //
    // TODO: What if key type differs between db file and specified key serializer?
    val mapDb = DBMaker.fileDB(dbPath).readOnly().closeOnJvmShutdown().make()
    mapDb.hashMap("map", getKeySerializer[K](), Serializer.STRING).open()
  }

  private[plugin] def isDbFile(dbPath: String, keyType: String): Boolean = try {
    val mapDb = keyType match {
      case "string" => openMapDb[String](dbPath)
      case "int" => openMapDb[Int](dbPath)
      case "long" => openMapDb[Long](dbPath)
      case t => throw new IllegalStateException(s"Unsupported type: $t")
    }
    mapDb.close()
    true
  } catch {
    case NonFatal(_) =>
      false
  }

  private def openAndGetMap[K: ClassTag](dbPath: String): K => String = try {
    val mapDb = openMapDb(dbPath)
    (key: K) => mapDb.get(key)
  } catch {
    case NonFatal(_) =>
      throw new RuntimeException(s"Cannot open a specified database: $dbPath")
  }

  private def createDictServer(
      keyType: String,
      dbPath: String,
      port: Int,
      cacheSize: Int,
      cacheConcurrencyLv: Int,
      keyTypeCheckEnabled: Boolean): DictServer = {
    def createServ[K: ClassTag]() = {
      DictServer(port, openAndGetMap[K](dbPath),
        cacheSize, cacheConcurrencyLv, keyTypeCheckEnabled)
    }
    keyType match {
      case "string" => createServ[String]()
      case "int" => createServ[Int]()
      case "long" => createServ[Long]()
      case t => throw new IllegalStateException(s"Unsupported type: $t")
    }
  }

  private[plugin] def isPortInUse(port: Int): Boolean = try {
    new ServerSocket(port, 0, InetAddress.getByName("localhost")).close()
    false
  } catch {
    case NonFatal(_) => true
  }

  private[plugin] def initRpcServ(conf: util.Map[String, String]): DictServer = {
    val keyType = conf.get("keyType")
    val port = conf.get("port").toInt
    val cacheSize = conf.get("mapCacheSize").toInt
    val cacheConcurrencyLv = conf.get("mapCacheConcurrencyLv").toInt
    val keyTypeCheckEnabled = conf.get("mapKeyTypeCheckEnabled").toBoolean
    val dbPath = {
      val path = conf.get("dbPath")
      if (path.isEmpty) {
        getDbPathFromSparkFiles()
      } else {
        path
      }
    }
    logInfo(s"port=$port dbPath=$dbPath keyType=${conf.get("keyType")} mapCacheSize=$cacheSize")
    createDictServer(keyType, dbPath, port, cacheSize, cacheConcurrencyLv, keyTypeCheckEnabled)
  }
}

object DictPluginConf {
  val EXECUTOR_DICT_DB_FILE = "spark.plugins.executorDict.dbFile"
  val EXECUTOR_DICT_PORT = "spark.plugins.executorDict.port"
  val EXECUTOR_DICT_MAP_KEY_TYPE = "spark.plugins.executorDict.keyType"
  val EXECUTOR_DICT_MAP_CACHE_SIZE = "spark.plugins.executorDict.mapCacheSize"
  val EXECUTOR_DICT_MAP_CACHE_CONCURRENCY_LEVEL =
    "spark.plugins.executorDict.mapCacheConcurrencyLv"
  val EXECUTOR_DICT_MAP_KEY_TYPE_CHECK_ENABLED =
    "spark.plugins.executorDict.mapKeyTypeCheckEnabled"
}

class SparkExecutorDictPlugin extends SparkPlugin with Logging {

  private[plugin] def dbPath(conf: SparkConf) = {
    val dbPath = conf.get(DictPluginConf.EXECUTOR_DICT_DB_FILE, "")
    if (dbPath.isEmpty) {
      val sparkFiles = conf.get("spark.files", null)
      if (sparkFiles == null) {
        throw new IllegalStateException("`spark.files` must contain .db file")
      }
      val dbFile = Utils.stringToSeq(sparkFiles)
        .filter(_.endsWith(".db")).map(Utils.resolveURI(_).getPath)
      if (dbFile.length != 1 ||
          !SparkExecutorDictPlugin.isDbFile(dbFile.head, keyType(conf))) {
        throw new IllegalStateException(
          s"`spark.files` must contain a single .db file, but got: " +
            dbFile.mkString(","))
      }
      ""
    } else {
      logWarning("Makes sure that all the executor hosts " +
        s"have the valid specified path: $dbPath")
      dbPath
    }
  }

  private[plugin] def port(conf: SparkConf) = {
    val port = conf.get(DictPluginConf.EXECUTOR_DICT_PORT, SparkExecutorDictPlugin.DEFAULT_PORT)
    if (Try(port.toInt).isFailure || port.toInt <= 0) {
      throw new IllegalStateException(s"`${DictPluginConf.EXECUTOR_DICT_PORT}` " +
        s"must be a positive number, but: $port")
    }
    port
  }

  private[plugin] def keyType(conf: SparkConf) = {
    val expectedKeyTypes = Set("string", "int", "long")
    val keyType = conf.get(DictPluginConf.EXECUTOR_DICT_MAP_KEY_TYPE, "string")
      .toLowerCase(util.Locale.ROOT)
    if (!expectedKeyTypes.contains(keyType)) {
      throw new IllegalStateException(s"`${DictPluginConf.EXECUTOR_DICT_MAP_KEY_TYPE}` " +
        s"must be one of ${expectedKeyTypes.mkString("/")}, but: $keyType")
    }
    keyType
  }

  private[plugin] def mapCacheSize(conf: SparkConf) = {
    val mapCacheSize = conf.get(DictPluginConf.EXECUTOR_DICT_MAP_CACHE_SIZE, "10000")
    if (Try(mapCacheSize.toInt).isFailure || mapCacheSize.toInt <= 0) {
      throw new IllegalStateException(s"`${DictPluginConf.EXECUTOR_DICT_MAP_CACHE_SIZE}` " +
        s"must be a positive number, but: $mapCacheSize")
    }
    mapCacheSize
  }

  private[plugin] def mapCacheConcurrencyLv(conf: SparkConf) = {
    val mapCacheConcurrencyLv = conf.get(
      DictPluginConf.EXECUTOR_DICT_MAP_CACHE_CONCURRENCY_LEVEL, "8")
    if (Try(mapCacheConcurrencyLv.toInt).isFailure || mapCacheConcurrencyLv.toInt <= 0) {
      throw new IllegalStateException(
        s"`${DictPluginConf.EXECUTOR_DICT_MAP_CACHE_CONCURRENCY_LEVEL}` " +
        s"must be a positive number, but: $mapCacheConcurrencyLv")
    }
    mapCacheConcurrencyLv
  }

  private[plugin] def mapKeyTypeCheckEnabled(conf: SparkConf) = {
    val mapKeyTypeCheckEnabled = conf.get(
      DictPluginConf.EXECUTOR_DICT_MAP_KEY_TYPE_CHECK_ENABLED, "true")
    if (Try(mapKeyTypeCheckEnabled.toBoolean).isFailure) {
      throw new IllegalStateException(
        s"`${DictPluginConf.EXECUTOR_DICT_MAP_KEY_TYPE_CHECK_ENABLED}` " +
        s"must be boolean, but: $mapKeyTypeCheckEnabled")
    }
    mapKeyTypeCheckEnabled
  }

  override def driverPlugin(): DriverPlugin = {
    new DriverPlugin {
      override def init(sc: SparkContext, ctx: PluginContext): util.Map[String, String] = {
        import collection.JavaConverters._
        Map(
          "dbPath" -> dbPath(sc.getConf),
          "port" -> port(sc.getConf),
          "keyType" -> keyType(sc.getConf),
          "mapCacheSize" -> mapCacheSize(sc.getConf),
          "mapCacheConcurrencyLv" -> mapCacheConcurrencyLv(sc.getConf),
          "mapKeyTypeCheckEnabled" -> mapKeyTypeCheckEnabled(sc.getConf)
        ).asJava
      }
    }
  }

  override def executorPlugin(): ExecutorPlugin = {
    new ExecutorPlugin() {
      var rpcServ: DictServer = _

      private def giveUpMsg(port: Int) = {
        s"executor-${SparkEnv.get.executorId} gave up starting up " +
          s"${DictServer.getClass.getSimpleName} due to the port '$port' already in use"
      }

      override def init(ctx: PluginContext, extraConf: util.Map[String, String]): Unit = {
        val port = extraConf.get("port").toInt
        if (!SparkExecutorDictPlugin.isPortInUse(port)) {
          rpcServ = Try(SparkExecutorDictPlugin.initRpcServ(extraConf)).getOrElse {
            logInfo(giveUpMsg(port))
            null
          }
        } else {
          logInfo(giveUpMsg(port))
        }
      }

      override def shutdown(): Unit = if (rpcServ != null) {
        try {
          rpcServ.shutdown()
        } catch {
          case NonFatal(e) =>
            logWarning(s"Cannot shutdown gracefully because: ${e.getMessage}")
        }
      }
    }
  }
}
