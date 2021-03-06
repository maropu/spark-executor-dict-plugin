#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import os
import signal
import shutil
import tempfile
import unittest
from contextlib import contextmanager

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import Row


def load_testdata(spark: SparkSession, filename, schema=None):
    """
    Load test data from a given file name and return it as 'DataFrame'
    """
    fmt = os.path.splitext(filename)[1][1:]
    reader = spark.read.schema(schema) if schema is not None \
        else spark.read.option("inferSchema", True)
    return reader.format(fmt) \
        .option("header", True) \
        .load("{}/{}".format(os.getenv("REPAIR_TESTDATA"), filename))


class TestTimeout(Exception):
    pass


class Eventually:

    def __init__(self, seconds, error_message=None):
        if error_message is None:
            error_message = 'test timed out after {}s.'.format(seconds)
        self.seconds = seconds
        self.error_message = error_message

    def handle_timeout(self, signum, frame):
        raise TestTimeout(self.error_message)

    def __enter__(self):
        signal.signal(signal.SIGALRM, self.handle_timeout)
        signal.alarm(self.seconds)

    def __exit__(self, exc_type, exc_val, exc_tb):
        signal.alarm(0)


class ReusedPySparkTestCase(unittest.TestCase):

    @classmethod
    def conf(cls):
        """
        Override this in subclasses to supply a more specific conf
        """
        return SparkConf()

    @classmethod
    def setUpClass(cls):
        cls.sc = SparkContext('local[4]', cls.__name__, conf=cls.conf())

    @classmethod
    def tearDownClass(cls):
        cls.sc.stop()


class SQLTestUtils(object):
    """
    This util assumes the instance of this to have 'spark' attribute, having a spark session.
    It is usually used with 'ReusedSQLTestCase' class but can be used if you feel sure the
    the implementation of this class has 'spark' attribute.
    """

    @contextmanager
    def sql_conf(self, pairs):
        """
        A convenient context manager to test some configuration specific logic. This sets
        `value` to the configuration `key` and then restores it back when it exits.
        """
        assert isinstance(pairs, dict), "pairs should be a dictionary."
        assert hasattr(self, "spark"), "it should have 'spark' attribute, having a spark session."

        keys = pairs.keys()
        new_values = pairs.values()
        old_values = [self.spark.conf.get(key, None) for key in keys]
        for key, new_value in zip(keys, new_values):
            self.spark.conf.set(key, new_value)
        try:
            yield
        finally:
            for key, old_value in zip(keys, old_values):
                if old_value is None:
                    self.spark.conf.unset(key)
                else:
                    self.spark.conf.set(key, old_value)

    @contextmanager
    def database(self, *databases):
        """
        A convenient context manager to test with some specific databases. This drops the given
        databases if it exists and sets current database to "default" when it exits.
        """
        assert hasattr(self, "spark"), "it should have 'spark' attribute, having a spark session."

        try:
            yield
        finally:
            for db in databases:
                self.spark.sql(f"DROP DATABASE IF EXISTS {db} CASCADE")
            self.spark.catalog.setCurrentDatabase("default")

    @contextmanager
    def table(self, *tables):
        """
        A convenient context manager to test with some specific tables. This drops the given tables
        if it exists.
        """
        assert hasattr(self, "spark"), "it should have 'spark' attribute, having a spark session."

        try:
            yield
        finally:
            for t in tables:
                self.spark.sql(f"DROP TABLE IF EXISTS {t}")

    @contextmanager
    def tempView(self, *views):
        """
        A convenient context manager to test with some specific views. This drops the given views
        if it exists.
        """
        assert hasattr(self, "spark"), "it should have 'spark' attribute, having a spark session."

        try:
            yield
        finally:
            for v in views:
                self.spark.catalog.dropTempView(v)

    @contextmanager
    def function(self, *functions):
        """
        A convenient context manager to test with some specific functions. This drops the given
        functions if it exists.
        """
        assert hasattr(self, "spark"), "it should have 'spark' attribute, having a spark session."

        try:
            yield
        finally:
            for f in functions:
                self.spark.sql(f"DROP FUNCTION IF EXISTS {f}")


class ReusedSQLTestCase(ReusedPySparkTestCase, SQLTestUtils):
    @classmethod
    def setUpClass(cls):
        super(ReusedSQLTestCase, cls).setUpClass()
        cls.spark = SparkSession(cls.sc)
        cls.tempdir = tempfile.NamedTemporaryFile(delete=False)
        os.unlink(cls.tempdir.name)
        cls.testData = [Row(key=i, value=str(i)) for i in range(100)]
        cls.df = cls.spark.createDataFrame(cls.testData)

    @classmethod
    def tearDownClass(cls):
        super(ReusedSQLTestCase, cls).tearDownClass()
        cls.spark.stop()
        shutil.rmtree(cls.tempdir.name, ignore_errors=True)
