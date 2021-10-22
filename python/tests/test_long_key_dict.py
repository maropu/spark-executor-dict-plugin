#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import os
import unittest

from pyspark import SparkConf
from pyspark.sql import Row

from tests.requirements import have_pandas, have_pyarrow, \
    pandas_requirement_message, pyarrow_requirement_message
from tests.testutils import ReusedSQLTestCase

from client import DictClient


@unittest.skipIf(
    not have_pandas or not have_pyarrow,
    pandas_requirement_message or pyarrow_requirement_message)  # type: ignore
class LongKeyDictTests(ReusedSQLTestCase):

    @classmethod
    def conf(cls):
        return SparkConf() \
            .set("spark.master", "local[1]") \
            .set("spark.driver.memory", "1g") \
            .set("spark.jars", os.getenv("DICT_API_LIB")) \
            .set("spark.plugins", "org.apache.spark.plugin.SparkExecutorDictPlugin") \
            .set("spark.files", "{}/long_key_dict.db".format(os.getenv("DICT_TESTDATA"))) \
            .set("spark.plugins.executorDict.port", "8002") \
            .set("spark.plugins.executorDict.keyType", "long")

    @classmethod
    def setUpClass(cls):
        super(LongKeyDictTests, cls).setUpClass()

        # Tunes # shuffle partitions
        num_parallelism = cls.spark.sparkContext.defaultParallelism
        cls.spark.sql(f"SET spark.sql.shuffle.partitions={num_parallelism}")

    @classmethod
    def tearDownClass(cls):
        super(ReusedSQLTestCase, cls).tearDownClass()

    def test_basics(self):
        from pyspark.sql.functions import col, udf

        @udf(returnType='string')
        def _udf(x):
            client = DictClient(port=8002)
            return str(client.lookup(x))

        df = self.spark.range(4).selectExpr("CAST(id AS LONG) id")
        df = df.select(_udf(col("id")).alias("value"))
        self.assertEqual(df.orderBy("value").collect(), [
            Row(value=""), Row(value="a"), Row(value="b"), Row(value="c")])

    def test_retry_errors(self):
        client = DictClient(num_retries=-1)
        self.assertRaises(RuntimeError, lambda: client.lookup_by_i64(1))
        self.assertRaises(RuntimeError, lambda: client.lookup(2))

if __name__ == "__main__":
    try:
        import xmlrunner
        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
