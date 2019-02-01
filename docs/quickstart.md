<!---
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->

# Spark's quick start with Yosegi

## Preparation
We have a plan to create a docker environment of Spark test use, but current situation, you need to prepare Spark firstly.

- [Apache Spark](https://spark.apache.org/)

## Yosegi's jars
Get Yosegi's jar from Maven repository.It can be easily obtained from the following command.

	$ ./bin/get_jar.sh get

It is in the following path.
```
./jars/yosegi/latest/yosegi.jar
```

Specify yosegi's jars for Spark's startup option "- jar".
```
$ ./bin/spark-shell --jars ./jars/yosegi/latest/yosegi.jar,./target/scala-2.11/yosegi-spark_2.11-1.0.jar
```

## Preparation of input data.
In this example, CSV data is assumed to be input data.
With this data as input, we create a table in Yosegi format and read data from the table.

Create the following csv file.In this example, to illustrate writing to and reading from the table, the data is a simple example.
If you already have a table, you still have the input.

```
{"id":"X_0001","name":"AAA","age":20}
{"id":"X_0002","name":"BBB","age":30}
{"id":"X_0003","name":"CCC","age":32}
{"id":"X_0004","name":"DDD","age":21}
{"id":"X_0005","name":"EEE","age":28}
{"id":"X_0006","name":"FFF","age":21}
```

## Create yosegi file

Run Spark.
Load json file and create yosegi file.

```
val sqlContext = new SQLContext(sc);
val df = sqlContext.read.json("/tmp/example.json")
```

JSON output looks like this.

```
scala> df.show()
+---+------+----+
|age|    id|name|
+---+------+----+
| 20|X_0001| AAA|
| 30|X_0002| BBB|
| 32|X_0003| CCC|
| 21|X_0004| DDD|
| 28|X_0005| EEE|
| 21|X_0006| FFF|
+---+------+----+
```

Save the data frame created from JSON as a yosegi file.

```
df.write.format("jp.co.yahoo.yosegi.spark.YosegiFileFormat").save("/tmp/example.yosegi")
```

## Read yosegi file

Read as a data frame from yosegi file.

```
val sqlContext = new SQLContext(sc);
val df = sqlContext.read.format("jp.co.yahoo.yosegi.spark.YosegiFileFormat" ).load( "/tmp/example.yosegi")
df.show
```

The output looks like this.

```
scala> df.show()
+----+------+---+
|name|    id|age|
+----+------+---+
| AAA|X_0001| 20|
| BBB|X_0002| 30|
| CCC|X_0003| 32|
| DDD|X_0004| 21|
| EEE|X_0005| 28|
| FFF|X_0006| 21|
+----+------+---+
```

# What if I want to know more?
