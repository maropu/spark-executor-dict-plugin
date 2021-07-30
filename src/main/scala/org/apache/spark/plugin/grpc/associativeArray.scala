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

import io.grpc.{ManagedChannelBuilder, Server, ServerBuilder}
import io.grpc.stub.StreamObserver

import org.apache.spark.internal.Logging
import org.apache.spark.plugin.grpc.AssociativeArrayGrpc._

class AssociativeArrayService(map: String => String)
  extends AssociativeArrayImplBase {

  override def lookup(req: LookupRequest, resObs: StreamObserver[ValueReply]): Unit = {
    // TODO: Cache valueReplay
    val value = map(req.getKey)
    val responseValue = if (value != null) {
      ValueReply.newBuilder().setValue(value).build()
    } else {
      ValueReply.newBuilder().setValue("").build()
    }
    resObs.onNext(responseValue)
    resObs.onCompleted()
  }
}

class AssociativeArrayServer private(serv: Server) extends Logging {
  serv.start()
  logInfo(s"${this.getClass.getSimpleName} started, listening on ${serv.getPort}")

  def this(port: Int, map: String => String) = {
    this(ServerBuilder
      .forPort(port)
      .addService(new AssociativeArrayService(map))
      .asInstanceOf[ServerBuilder[_]]
      .build())
  }

  def shutdown(): Unit = serv.shutdownNow()
}

object AssociativeArrayServer {
  val DEFAULT_PORT = 6543
}

class KvsClient private(stub: AssociativeArrayBlockingStub) extends Logging {

  def this(port: Int = AssociativeArrayServer.DEFAULT_PORT) = {
    this(AssociativeArrayGrpc.newBlockingStub(ManagedChannelBuilder
      .forAddress("localhost", port)
      .usePlaintext()
      .asInstanceOf[ManagedChannelBuilder[_]]
      .build()))
  }

  def lookup(key: String): String = {
    val lookupReq = LookupRequest.newBuilder().setKey(key).build()
    try {
      stub.lookup(lookupReq).getValue
    } catch {
      case NonFatal(e) =>
        logWarning(s"RPC failed: ${e.getMessage}")
        ""
    }
  }
}