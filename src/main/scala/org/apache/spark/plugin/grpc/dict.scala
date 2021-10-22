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

package org.apache.spark.plugin.grpc

import java.util.concurrent.{Executors, TimeUnit}

import scala.reflect.{classTag, ClassTag}
import scala.util.control.NonFatal

import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache}
import io.grpc.{ManagedChannelBuilder, Server, ServerBuilder}
import io.grpc.stub.StreamObserver

import org.apache.spark.internal.Logging
import org.apache.spark.plugin.SparkExecutorDictPlugin
import org.apache.spark.plugin.grpc.DictGrpc._

abstract class BaseDictService[K: ClassTag] extends DictImplBase with Logging {
  def map: K => String
  def cacheSize: Int
  def cacheConcurrencyLv: Int
  def keyTypeCheckEnabled: Boolean

  protected def getKey(req: LookupRequest): K

  // Guava cache should be thread-safe:
  // https://guava.dev/releases/21.0/api/docs/com/google/common/cache/LoadingCache.html
  protected val cache: LoadingCache[AnyRef, ValueReply] = CacheBuilder.newBuilder()
    .initialCapacity(cacheSize)
    .maximumSize(cacheSize)
    .concurrencyLevel(cacheConcurrencyLv)
    .recordStats()
    .build(
      new CacheLoader[AnyRef, ValueReply]() {
        override def load(key: AnyRef): ValueReply = {
          val maybeValue = map(key.asInstanceOf[K])
          val value = if (maybeValue != null) maybeValue else ""
          ValueReply.newBuilder().setValue(value).build()
        }
      })

  Executors.newSingleThreadScheduledExecutor()
    .scheduleWithFixedDelay(new Runnable {
      override def run(): Unit = logInfo(cacheStats())
    }, 10L, 10L, TimeUnit.MINUTES)

  // TODO: Needs to send the stats into driver-side metric system
  private def cacheStats(): String = {
    s"hitRate=${cache.stats().hitRate()} missRate=${cache.stats().hitRate()} " +
      s"requestCnt=${cache.stats().requestCount()}"
  }

  override def lookup(req: LookupRequest, resObs: StreamObserver[ValueReply]): Unit = {
    val responseValue = cache.get(getKey(req).asInstanceOf[AnyRef])
    resObs.onNext(responseValue)
    resObs.onCompleted()
  }
}

class StringKeyDictService(
    override val map: String => String,
    override val cacheSize: Int,
    override val cacheConcurrencyLv: Int,
    override val keyTypeCheckEnabled: Boolean)
  extends BaseDictService[String] {

  private lazy val getKeyImpl: LookupRequest => String = if (keyTypeCheckEnabled) {
    (req: LookupRequest) => {
      if (req.hasInt32Key || req.hasInt64Key) {
        throw new RuntimeException("Incompatible key type found and it must be string")
      }
      req.getStrKey
    }
  } else {
    (req: LookupRequest) => req.getStrKey
  }

  override protected def getKey(req: LookupRequest): String = {
    getKeyImpl(req)
  }
}

class IntKeyDictService(
    override val map: Int => String,
    override val cacheSize: Int,
    override val cacheConcurrencyLv: Int,
    override val keyTypeCheckEnabled: Boolean)
  extends BaseDictService[Int] {

  private lazy val getKeyImpl: LookupRequest => Int = if (keyTypeCheckEnabled) {
    (req: LookupRequest) => {
      if (req.hasStrKey || req.hasInt64Key) {
        throw new RuntimeException("Incompatible key type found and it must be int")
      }
      req.getInt32Key
    }
  } else {
    (req: LookupRequest) => req.getInt32Key
  }

  override protected def getKey(req: LookupRequest): Int = {
    getKeyImpl(req)
  }
}

class LongKeyDictService(
    override val map: Long => String,
    override val cacheSize: Int,
    override val cacheConcurrencyLv: Int,
    override val keyTypeCheckEnabled: Boolean)
  extends BaseDictService[Long] {

  private lazy val getKeyImpl: LookupRequest => Long = if (keyTypeCheckEnabled) {
    (req: LookupRequest) => {
      if (req.hasStrKey || req.hasInt32Key) {
        throw new RuntimeException("Incompatible key type found and it must be long")
      }
      req.getInt64Key
    }
  } else {
    (req: LookupRequest) => req.getInt64Key
  }

  override protected def getKey(req: LookupRequest): Long = {
    getKeyImpl(req)
  }
}

class DictServer private(serv: Server) extends Logging {
  serv.start()
  logInfo(s"${this.getClass.getSimpleName} started, listening on ${serv.getPort}")

  private[grpc] def this(port: Int, service: BaseDictService[_]) = {
    this(ServerBuilder
      .forPort(port)
      .addService(service)
      .asInstanceOf[ServerBuilder[_]]
      .build())
  }

  def shutdown(): Unit = {
    val isTerminated = serv.shutdown().awaitTermination(10L, TimeUnit.SECONDS)
    assert(isTerminated)
  }
}

object DictServer {

  def apply[K: ClassTag](
      port: Int,
      map: K => String,
      cacheSize: Int,
      cacheConcurrencyLv: Int,
      keyTypeCheckEnabled: Boolean): DictServer = {
    new DictServer(port, classTag[K].runtimeClass match {
      case c: Class[_] if c == classOf[String] =>
        new StringKeyDictService(map.asInstanceOf[String => String],
          cacheSize, cacheConcurrencyLv, keyTypeCheckEnabled)
      case c: Class[_] if c == java.lang.Integer.TYPE =>
        new IntKeyDictService(map.asInstanceOf[Int => String],
          cacheSize, cacheConcurrencyLv, keyTypeCheckEnabled)
      case c: Class[_] if c == java.lang.Long.TYPE =>
        new LongKeyDictService(map.asInstanceOf[Long => String],
          cacheSize, cacheConcurrencyLv, keyTypeCheckEnabled)
      case c =>
        throw new RuntimeException(s"Unsupported Type: ${c.getSimpleName}")
    })
  }
}

abstract class BaseDictClient[K: ClassTag](stub: DictBlockingStub) extends Logging {

  def numRetries: Int
  def errorOnRpcFailure: Boolean

  stub.withWaitForReady()

  protected def buildLookupRequest(key: K): LookupRequest

  def lookup(key: K): String = {
    val lookupReq = buildLookupRequest(key)
    var numTrials = 0
    while (numTrials <= numRetries) {
      numTrials += 1
      try {
        return stub.lookup(lookupReq).getValue
      } catch {
        case NonFatal(e) if numTrials >= numRetries && errorOnRpcFailure =>
          throw new RuntimeException(s"RPC failed: ${e.getMessage}", e)
        case NonFatal(_) =>
      }
    }
    logInfo(s"RPC (key=${key} lookup) failed " +
      s"because the maximum number of retries (numRetries=$numRetries) reached")
    ""
  }
}

private[grpc] class StringKeyDictClient(
    stub: DictBlockingStub,
    override val numRetries: Int,
    override val errorOnRpcFailure: Boolean) extends BaseDictClient[String](stub) {

  override protected def buildLookupRequest(key: String): LookupRequest = {
    LookupRequest.newBuilder().setStrKey(key).build()
  }
}

private[grpc] class IntKeyDictClient(
    stub: DictBlockingStub,
    override val numRetries: Int,
    override val errorOnRpcFailure: Boolean) extends BaseDictClient[Int](stub) {

  override protected def buildLookupRequest(key: Int): LookupRequest = {
    LookupRequest.newBuilder().setInt32Key(key).build()
  }
}

private[grpc] class LongKeyDictClient(
    stub: DictBlockingStub,
    override val numRetries: Int,
    override val errorOnRpcFailure: Boolean) extends BaseDictClient[Long](stub) {

  override protected def buildLookupRequest(key: Long): LookupRequest = {
    LookupRequest.newBuilder().setInt64Key(key).build()
  }
}

object DictClient {

  private def initDictStub(port: Int): DictBlockingStub = {
    DictGrpc.newBlockingStub(ManagedChannelBuilder
      .forAddress("localhost", port)
      .usePlaintext()
      .asInstanceOf[ManagedChannelBuilder[_]]
      .build())
  }

  def apply[K: ClassTag](
      port: Int = SparkExecutorDictPlugin.DEFAULT_PORT.toInt,
      numRetries: Int = 3,
      errorOnRpcFailure: Boolean = true): BaseDictClient[K] = {
    val dictStub = initDictStub(port)
    val client = classTag[K].runtimeClass match {
      case c: Class[_] if c == classOf[String] =>
        new StringKeyDictClient(dictStub, numRetries, errorOnRpcFailure)
      case c: Class[_] if c == java.lang.Integer.TYPE =>
        new IntKeyDictClient(dictStub, numRetries, errorOnRpcFailure)
      case c: Class[_] if c == java.lang.Long.TYPE =>
        new LongKeyDictClient(dictStub, numRetries, errorOnRpcFailure)
      case c =>
        throw new RuntimeException(s"Unsupported Type: ${c.getSimpleName}")
    }
    client.asInstanceOf[BaseDictClient[K]]
  }
}