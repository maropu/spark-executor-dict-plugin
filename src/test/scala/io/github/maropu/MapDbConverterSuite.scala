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

import scala.reflect.ClassTag

import org.mapdb.{DBMaker, HTreeMap, Serializer}

import org.apache.spark.SparkFunSuite

class MapDbConverterSuite extends SparkFunSuite {

  def testSaveFunc[K: ClassTag](saveAndGetTestData: String => Map[K, String]): Unit = {
    withTempDir { tempDir =>
      val dbPath = s"${tempDir.getAbsolutePath}/test.db"
      val expectedMap = saveAndGetTestData(dbPath)
      val mapDb = DBMaker.fileDB(dbPath).readOnly().make()
      val map = mapDb
        .hashMap("map", MapDbConverter.getKeySerializer[K](), Serializer.STRING)
        .open().asInstanceOf[HTreeMap[K, String]]
      expectedMap.foreach { case (k, v) =>
        assert(map.get(k) === v)
      }
      mapDb.close()
    }
  }

  test("save - string key") {
    testSaveFunc { dbPath =>
      val data = Map("1" -> "a", "2" -> "b")
      MapDbConverter.save(dbPath, data)
      data
    }
    testSaveFunc { dbPath =>
      val data = Map("3" -> "c", "4" -> "d", "5" -> "e")
      MapDbConverter.save(dbPath, data.toSeq)
      data
    }
    testSaveFunc { dbPath =>
      val data = Map("6" -> "f", "7" -> "g")
      MapDbConverter.save(dbPath, data.toIterator)
      data
    }
  }

  test("save - int key") {
    testSaveFunc { dbPath =>
      val data = Map(1 -> "a", 2 -> "b")
      MapDbConverter.save(dbPath, data)
      data
    }
    testSaveFunc { dbPath =>
      val data = Map(3 -> "c", 4 -> "d", 5 -> "e")
      MapDbConverter.save(dbPath, data.toSeq)
      data
    }
    testSaveFunc { dbPath =>
      val data = Map(6 -> "f", 7 -> "g")
      MapDbConverter.save(dbPath, data.toIterator)
      data
    }
  }

  test("save - long key") {
    testSaveFunc { dbPath =>
      val data = Map(1L -> "a", 2L -> "b")
      MapDbConverter.save(dbPath, data)
      data
    }
    testSaveFunc { dbPath =>
      val data = Map(3L -> "c", 4L -> "d", 5L -> "e")
      MapDbConverter.save(dbPath, data.toSeq)
      data
    }
    testSaveFunc { dbPath =>
      val data = Map(6L -> "f", 7L -> "g")
      MapDbConverter.save(dbPath, data.toIterator)
      data
    }
  }
}
