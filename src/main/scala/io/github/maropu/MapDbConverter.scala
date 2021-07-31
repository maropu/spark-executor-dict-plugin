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

package io.github.maropu

import scala.collection.JavaConverters._

import org.mapdb.{DB, DBMaker, HTreeMap, Serializer}

object MapDbConverter {

  private def openMapDb(path: String): (DB, HTreeMap[String, String]) = {
    val mapDb = DBMaker.fileDB(path).make()
    val map: HTreeMap[String, String] = mapDb
      .hashMap("map", Serializer.STRING, Serializer.STRING)
      .create()
    (mapDb, map)
  }

  def save(path: String, data: Map[String, String]): Unit = {
    val (mapDb, map) = openMapDb(path)
    map.putAll(data.asJava)
    mapDb.close()
  }

  def save(path: String, data: Seq[(String, String)]): Unit = {
    save(path, data.toMap)
  }

  def save(path: String, iterator: Iterator[(String, String)]): Unit = {
    val (mapDb, map) = openMapDb(path)
    iterator.foreach { case (k, v) => map.put(k, v) }
    mapDb.close()
  }
}
