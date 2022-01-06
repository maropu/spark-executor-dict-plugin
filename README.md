[![License](http://img.shields.io/:license-Apache_v2-blue.svg)](https://github.com/maropu/spark-executor-dict-plugin/blob/master/LICENSE)
[![Build and test](https://github.com/maropu/spark-executor-dict-plugin/workflows/Build%20and%20test/badge.svg)](https://github.com/maropu/spark-executor-dict-plugin/actions?query=workflow%3A%22Build+and+test%22)

This repository provides a Spark plugin implementation for executor-side RPC dict servers where a user can look up values with associated keys.
There is the use case where a user wants to compute values in a map task by referring to a static shared state (e.g., a pre-built knowledge base and master data).
If a shared state is small, a broadcast variable is a good fit for the case as follows:

```
>>> from pyspark.sql.functions import udf
>>> broadcasted_hmap = spark.sparkContext.broadcast({"key1": "value1", "key2": "value2", ...})
>>> @udf(returnType='string')
... def udf(x):
...     hmap = broadcasted_hmap.value
...     return hmap[x]
...
>>> import pandas as pd
>>> df = spark.createDataFrame(pd.DataFrame({'x': ['key1', 'key2']}))
>>> df = df.select(udf("x"))
>>> df.show()
+------+
|udf(x)|
+------+
|value1|
|value2|
+------+
```

Having a copied state on each task's memory, however, can be wasteful if the state is pretty big (e.g., 10g or more).
To mitigate the issue, this plugin enables a user to spin up a RPC server along with an executor and
the RPC server will return values associated with keys by referring to a specified state.
Since all map tasks in an executor access the same shared state in a RPC server,
the memory consumption is much smaller than that of broadcast variables.
How a user accesses a shared state via a RPC server is as follows:

```
# 'largeMap.db' is a MapDB file-backed hash map implementation, https://mapdb.org
$ pyspark --jars=./assembly/spark-executor-dict-plugin_2.12_spark3.1-0.1.0-SNAPSHOT-with-dependencies.jar \
  --py-files ./assembly/dict.zip \
  --conf spark.plugins=org.apache.spark.plugin.SparkExecutorDictPlugin \
  --conf spark.files=/tmp/largeMap.db

>>> from pyspark.sql.functions import udf
>>> @udf(returnType='string')
... def udf(x):
...     from client import DictClient
...     hmap = DictClient()
...     return hmap.lookup(x)
...
>>> import pandas as pd
>>> df = spark.createDataFrame(pd.DataFrame({'x': ['key1', 'key2']}))
>>> df = df.select(udf("x"))
>>> df.show()
+------+
|udf(x)|
+------+
|value1|
|value2|
+------+
```

A RPC server holds a shared state as an on-disk hash map that [MapDB](https://mapdb.org) provides.
Therefore, frequently-accessed key-value pairs are expected to be on memory and the memory footprint can be small.
For actual running examples, please see [test code](./python/tests/test_dict.py).

## MapDB data conversion

To generate a MapDB's map file for your data, you can use a helper function included in the package:

```
$ spark-shell --jars=./assembly/spark-executor-dict-plugin_2.12_spark3.1-0.1.0-SNAPSHOT-with-dependencies.jar

scala> import io.github.maropu.MapDbConverter
scala> val largeMap = Map("key1" -> "value1", "key2" -> "value2", ...)
scala> MapDbConverter.save("/tmp/largeMap.db", largeMap)
```

## Fixed-length lookup key

You can use fixed-length key types (`int`/`long`) in map data instead of a variable-length `string` one.
How one uses `long` keys in map data is as folows:

```
# Generate MapDB's map data whose key type is `long`
scala> import io.github.maropu.MapDbConverter
scala> val longKeyMap = Map(1L -> "value1", 2L -> "value2", ...)
scala> MapDbConverter.save("/tmp/longKeyMap.db", longKeyMap)

# To load the generated map data above, you need to set `long`
# to `spark.plugins.executorDict.keyType`
$ pyspark --jars=./assembly/spark-executor-dict-plugin_2.12_spark3.1-0.1.0-SNAPSHOT-with-dependencies.jar \
  --py-files ./assembly/dict.zip \
  --conf spark.plugins=org.apache.spark.plugin.SparkExecutorDictPlugin \
  --conf spark.files=/tmp/longKeyMap.db \
  --conf spark.plugins.executorDict.keyType=long

>>> from pyspark.sql.functions import udf
>>> @udf(returnType='string')
... def udf(x):
...     from client import DictClient
...     hmap = DictClient()
...     return hmap.lookup(x)
...
>>> import pandas as pd
>>> df = spark.createDataFrame(pd.DataFrame({'x': [1, 2]}))
>>> df = df.select(udf("x"))
>>> df.show()
+------+
|udf(x)|
+------+
|value1|
|value2|
+------+
```

### Configurations

| Property Name | Default | Meaning |
| ---- | ---- | ---- |
| spark.plugins.executorDict.dbFile | "" | Absolute path of a MapDB's loadable file in executor's instances. If not specified, you must pass it via `spark.files` instead. |
| spark.plugins.executorDict.port | 6543 | Default port number for a RPC dict server in an executor. |
| spark.plugins.executorDict.mapCacheSize | 10000 | Maximum number of cache entries for a shared dict. |
| spark.plugins.executorDict.keyType | "string" | Key type of a specified database file. This value must be one of string/int/long. |
| spark.plugins.executorDict.mapKeyTypeCheckEnabled | true | Specifies whether an exception is thrown if an incompatible lookup key detected. |

## TODO

 * Report some metrics via `MetricRegistry`
 * Adds more tests

## Bug reports

If you hit some bugs and requests, please leave some comments on [Issues](https://github.com/maropu/spark-executor-dict-plugin/issues)
or Twitter ([@maropu](http://twitter.com/#!/maropu)).

