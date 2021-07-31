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

import scala.util.control.NonFatal

import com.google.common.cache.{CacheBuilder, CacheLoader}
import io.grpc.{ManagedChannelBuilder, Server, ServerBuilder}
import io.grpc.stub.StreamObserver

import org.apache.spark.internal.Logging
import org.apache.spark.plugin.SparkExecutorDictPlugin
import org.apache.spark.plugin.grpc.DictGrpc._

class DictService(map: String => String, cacheSize: Int)
  extends DictImplBase {

  // Guava cache should be thread-safe:
  // https://guava.dev/releases/21.0/api/docs/com/google/common/cache/LoadingCache.html
  val cache = CacheBuilder.newBuilder()
    .maximumSize(cacheSize)
    .build(
      new CacheLoader[String, ValueReply]() {
        override def load(key: String): ValueReply = {
          val value = map(key)
          if (value != null) {
            ValueReply.newBuilder().setValue(value).build()
          } else {
            ValueReply.newBuilder().setValue("").build()
          }
        }
      })

  override def lookup(req: LookupRequest, resObs: StreamObserver[ValueReply]): Unit = {
    val responseValue = cache.get(req.getKey())
    resObs.onNext(responseValue)
    resObs.onCompleted()
  }
}

class DictServer private(serv: Server) extends Logging {
  serv.start()
  logInfo(s"${this.getClass.getSimpleName} started, listening on ${serv.getPort}")

  def this(port: Int, map: String => String, cacheSize: Int) = {
    this(ServerBuilder
      .forPort(port)
      .addService(new DictService(map, cacheSize))
      .asInstanceOf[ServerBuilder[_]]
      .build())
  }

  def shutdown(): Unit = serv.shutdownNow()
}

class DictClient private(
    stub: DictBlockingStub,
    numRetries: Int,
    errorOnRpcFailure: Boolean) extends Logging {

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
