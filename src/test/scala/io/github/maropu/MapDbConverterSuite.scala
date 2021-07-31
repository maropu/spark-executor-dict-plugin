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

import org.mapdb.{DBMaker, Serializer}

import org.apache.spark.SparkFunSuite

class MapDbConverterSuite extends SparkFunSuite {

  def testSaveFunc(save: (String, Map[String, String]) => Unit): Unit = {
    withTempDir { tempDir =>
      val dbPath = s"${tempDir.getAbsolutePath}/test.db"
      val data = Map("1" -> "a", "2" -> "b")
      save(dbPath, data)
      val mapDb = DBMaker.fileDB(dbPath).readOnly().make()
      val map = mapDb
        .hashMap("map", Serializer.STRING, Serializer.STRING)
        .open()
      assert(map.get("1") === "a")
      assert(map.get("2") === "b")
      assert(map.get("3") === null)
      mapDb.close()
    }
  }

  test("save") {
    testSaveFunc { case (dbPath, data) => MapDbConverter.save(dbPath, data) }
    testSaveFunc { case (dbPath, data) => MapDbConverter.save(dbPath, data.toSeq) }
    testSaveFunc { case (dbPath, data) => MapDbConverter.save(dbPath, data.toIterator) }
  }
}
