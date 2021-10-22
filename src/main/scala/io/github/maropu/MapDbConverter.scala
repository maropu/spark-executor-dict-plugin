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
import scala.reflect.{classTag, ClassTag}

import org.mapdb.{DB, DBMaker, HTreeMap, Serializer}

object MapDbConverter {

  private[maropu] def getKeySerializer[K: ClassTag]() = classTag[K].runtimeClass match {
    case c: Class[_] if c == classOf[String] => Serializer.STRING
    case c: Class[_] if c == java.lang.Integer.TYPE => Serializer.INTEGER
    case c: Class[_] if c == java.lang.Long.TYPE => Serializer.LONG
    case c => throw new RuntimeException(s"Unsupported type: ${c.getSimpleName}")
  }

  private def openMapDb[K: ClassTag](path: String): (DB, HTreeMap[K, String]) = {
    val mapDb = DBMaker.fileDB(path).make()
    val map = mapDb
      .hashMap("map", getKeySerializer[K](), Serializer.STRING)
      .create()
    (mapDb, map.asInstanceOf[HTreeMap[K, String]])
  }

  def save[K: ClassTag](path: String, data: Map[K, String]): Unit = {
    val (mapDb, map) = openMapDb[K](path)
    map.putAll(data.asJava)
    mapDb.close()
  }

  def save[K: ClassTag](path: String, data: Seq[(K, String)]): Unit = {
    save(path, data.toMap)
  }

  def save[K: ClassTag](path: String, iterator: Iterator[(K, String)]): Unit = {
    val (mapDb, map) = openMapDb[K](path)
    iterator.foreach { case (k, v) => map.put(k, v) }
    mapDb.close()
  }
}
