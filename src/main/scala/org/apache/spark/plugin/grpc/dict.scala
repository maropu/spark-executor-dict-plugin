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

import scala.util.control.NonFatal

import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache}
import io.grpc.{ManagedChannelBuilder, Server, ServerBuilder}
import io.grpc.stub.StreamObserver

import org.apache.spark.internal.Logging
import org.apache.spark.plugin.SparkExecutorDictPlugin
import org.apache.spark.plugin.grpc.DictGrpc._

class DictService(map: String => String, cacheSize: Int, cacheConcurrencyLv: Int)
  extends DictImplBase with Logging {

  // Guava cache should be thread-safe:
  // https://guava.dev/releases/21.0/api/docs/com/google/common/cache/LoadingCache.html
  val cache: LoadingCache[String, ValueReply] = CacheBuilder.newBuilder()
    .initialCapacity(cacheSize)
    .maximumSize(cacheSize)
    .concurrencyLevel(cacheConcurrencyLv)
    .recordStats()
    .build(
      new CacheLoader[String, ValueReply]() {
        override def load(key: String): ValueReply = {
          val maybeValue = map(key)
          val value = if (maybeValue != null) maybeValue else ""
          ValueReply.newBuilder().setValue(value).build()
        }
      })

  Executors.newSingleThreadScheduledExecutor()
      .scheduleWithFixedDelay(new Runnable {
    override def run(): Unit = logInfo(cacheStats())
  }, 10L, 10L, TimeUnit.MINUTES)

  // TODO: Needs to send the stats into driver-side metric system
  def cacheStats(): String = {
    s"hitRate=${cache.stats().hitRate()} missRate=${cache.stats().hitRate()} " +
      s"requestCnt=${cache.stats().requestCount()}"
  }

  override def lookup(req: LookupRequest, resObs: StreamObserver[ValueReply]): Unit = {
    val responseValue = cache.get(req.getKey())
    resObs.onNext(responseValue)
    resObs.onCompleted()
  }
}

class DictServer private(serv: Server, service: DictService) extends Logging {
  serv.start()
  logInfo(s"${this.getClass.getSimpleName} started, listening on ${serv.getPort}")

  def this(port: Int, service: DictService) = {
    this(ServerBuilder
      .forPort(port)
      .addService(service)
      .asInstanceOf[ServerBuilder[_]]
      .build(),
      service)
  }

  def this(port: Int, map: String => String, cacheSize: Int, cacheConcurrencyLv: Int) = {
    this(port, new DictService(map, cacheSize, cacheConcurrencyLv))
  }

  def shutdown(): Unit = {
    logInfo(service.cacheStats())
    val isTerminated = serv.shutdown().awaitTermination(10L, TimeUnit.SECONDS)
    assert(isTerminated)
  }
}

class DictClient private(
    stub: DictBlockingStub,
    numRetries: Int,
    errorOnRpcFailure: Boolean) extends Logging {

  stub.withWaitForReady()

  def this(
      port: Int = SparkExecutorDictPlugin.DEFAULT_PORT.toInt,
      numRetries: Int = 3,
      errorOnRpcFailure: Boolean = true) = {
    this(DictGrpc.newBlockingStub(ManagedChannelBuilder
        .forAddress("localhost", port)
        .usePlaintext()
        .asInstanceOf[ManagedChannelBuilder[_]]
        .build()),
      numRetries,
      errorOnRpcFailure)
  }

  def lookup(key: String): String = {
    val lookupReq = LookupRequest.newBuilder().setKey(key).build()
    var numTrials = 0
    while (numTrials <= numRetries) {
      numTrials += 1
      try {
        return stub.lookup(lookupReq).getValue
      } catch {
        case NonFatal(e) if numTrials >= numRetries && errorOnRpcFailure =>
          throw new RuntimeException(s"RPC failed: ${e.getMessage}")
        case NonFatal(_) =>
      }
    }
    logInfo(s"RPC (key=${key} lookup) failed " +
      s"because the maximum number of retries (numRetries=$numRetries) reached")
    ""
  }
}
