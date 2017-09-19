# tsfile-spark-connector

Used to read and write(developing) tsfile in spark.

将一个或多个TsFile展示成SparkSQL中的一张表。允许指定单个目录，或使用通配符匹配多个目录。如果是多个TsFile，schema将保留各个TsFile中sensor的并集。


## 1. dependency

https://github.com/thulab/tsfile.git


## 2. versions

The versions required for Spark and Java are as follow:

| Spark Version | Scala Version | Java Version |
| ------------- | ------------- | ------------ |
| `2.0+`        | `2.11`        | `1.8`        |



## 3. TsFile Type <=> SparkSQL type

This library uses the following mapping the data type from TsFile to SparkSQL:

| TsFile 		   | SparkSQL|
| --------------| -------------- |
| INT32       		   | IntegerType    |
| INT64       		   | LongType       |
| FLOAT       		   | FloatType      |
| DOUBLE      		   | DoubleType     |
| BYTE_ARRAY      		| StringType     |


## 4. TsFile Schema <-> SparkSQL Table Structure

The set of time-series data in section "Time-series Data" is used here to illustrate the mapping from TsFile Schema to SparkSQL Table Stucture.

<center>
<table style="text-align:center">
	<tr><th colspan="6">delta_object:root.car.turbine1</th></tr>
	<tr><th colspan="2">sensor_1</th><th colspan="2">sensor_2</th><th colspan="2">sensor_3</th></tr>
	<tr><th>time</th><th>value</td><th>time</th><th>value</td><th>time</th><th>value</td>
	<tr><td>1</td><td>1.2</td><td>1</td><td>20</td><td>2</td><td>50</td></tr>
	<tr><td>3</td><td>1.4</td><td>2</td><td>20</td><td>4</td><td>51</td></tr>
	<tr><td>5</td><td>1.1</td><td>3</td><td>21</td><td>6</td><td>52</td></tr>
	<tr><td>7</td><td>1.8</td><td>4</td><td>20</td><td>8</td><td>53</td></tr>
</table>
<span>A set of time-series data</span>
</center>

### 4.1. using delta_object as reserved column

There are two reserved columns in Spark SQL Table:

- `time` : Timestamp, LongType
- `delta_object` : Delta_object ID, StringType

The SparkSQL Table Structure is as follow:

<center>
	<table style="text-align:center">
	<tr><th>time(LongType)</th><th> delta_object(StringType)</th><th>sensor_1(FloatType)</th><th>sensor_2(IntType)</th><th>sensor_3(IntType)</th></tr>
	<tr><td>1</td><td> root.car.turbine1 </td><td>1.2</td><td>20</td><td>null</td></tr>
	<tr><td>2</td><td> root.car.turbine1 </td><td>null</td><td>20</td><td>50</td></tr>
	<tr><td>3</td><td> root.car.turbine1 </td><td>1.4</td><td>21</td><td>null</td></tr>
	<tr><td>4</td><td> root.car.turbine1 </td><td>null</td><td>20</td><td>51</td></tr>
	<tr><td>5</td><td> root.car.turbine1 </td><td>1.1</td><td>null</td><td>null</td></tr>
	<tr><td>6</td><td> root.car.turbine1 </td><td>null</td><td>null</td><td>52</td></tr>
	<tr><td>7</td><td> root.car.turbine1 </td><td>1.8</td><td>null</td><td>null</td></tr>
	<tr><td>8</td><td> root.car.turbine1 </td><td>null</td><td>null</td><td>53</td></tr>
	</table>

</center>


### 4.2. unfolding delta_object column

If you want to unfold the delta_object column into multi columns you should add an option when read and write:

e.g. 

option("delta_object_name" -> "root.device.turbine")

The "delta_object_name" is reserved key.


Then The SparkSQL Table Structure is as follow:

<center>
	<table style="text-align:center">
	<tr><th>time(LongType)</th><th> device(StringType)</th><th> turbine(StringType)</th><th>sensor_1(FloatType)</th><th>sensor_2(IntType)</th><th>sensor_3(IntType)</th></tr>
	<tr><td>1</td><td> car </td><td> turbine1 </td><td>1.2</td><td>20</td><td>null</td></tr>
	<tr><td>2</td><td> car </td><td> turbine1 </td><td>null</td><td>20</td><td>50</td></tr>
	<tr><td>3</td><td> car </td><td> turbine1 </td><td>1.4</td><td>21</td><td>null</td></tr>
	<tr><td>4</td><td> car </td><td> turbine1 </td><td>null</td><td>20</td><td>51</td></tr>
	<tr><td>5</td><td> car </td><td> turbine1 </td><td>1.1</td><td>null</td><td>null</td></tr>
	<tr><td>6</td><td> car </td><td> turbine1 </td><td>null</td><td>null</td><td>52</td></tr>
	<tr><td>7</td><td> car </td><td> turbine1 </td><td>1.8</td><td>null</td><td>null</td></tr>
	<tr><td>8</td><td> car </td><td> turbine1 </td><td>null</td><td>null</td><td>53</td></tr>
	</table>

</center>


Then you can group by any level in delta_object. And then with the same option you can write this dataframe to TsFile.

## 5. Building

```
mvn clean scala:compile compile package
```


## 6. Examples

### 6.1 Scala API

* **Example 1**

	```scala
	import cn.edu.thu.tsfile._
	import org.apache.spark.sql.SparkSession

	val spark = SparkSession.builder().master("local").getOrCreate()

	//read data in TsFile and create a table
	val df = spark.read.tsfile("test.tsfile")
	df.createOrReplaceTempView("TsFile_table")

	//query with filter
	val newDf = spark.sql("select * from TsFile_table where sensor_1 > 1.2").cache()

	newDf.show()

	```

* **Example 2**

	```scala
	import cn.edu.thu.tsfile._
    import org.apache.spark.sql.SparkSession
	
    val spark = SparkSession.builder().master("local").getOrCreate()
	val df = spark.read
	      .format("cn.edu.thu.tsfile")
	      .load("test.tsfile")
	df.filter("time < 10").show()

	```

* **Example 3**

	```scala
	import cn.edu.thu.tsfile._
    import org.apache.spark.sql.SparkSession
   	
	val spark = SparkSession.builder().master("local").getOrCreate()

	//create a table in SparkSQL and build relation with a TsFile
	spark.sql("create temporary view TsFile using cn.edu.thu.tsfile options(path = \"test.tsfile\")")

	spark.sql("select * from TsFile where sensor_1 > 1.2").show()

	```
	
* **Example 4(using options to read)**

	```scala
	import cn.edu.thu.tsfile._
	import org.apache.spark.sql.SparkSession
	import scala.collection.mutable

	val spark = SparkSession.builder().master("local").getOrCreate()
		
	val df = spark.read.option("delta_object_name", "root.device.turbine").tsfile("test.tsfile")
	    
	//create a table in SparkSQL and build relation with a TsFile
	df.createOrReplaceTempView("tsfile_table")
	
	spark.sql("select * from tsfile_table where turbine = 'turbine1' and device = 'car' and time < 10").show()
	```

* **Example 5(write)**

	```scala
	import cn.edu.thu.tsfile._
    import org.apache.spark.sql.SparkSession
   	
	val spark = SparkSession.builder().master("local").getOrCreate()

	val df = spark.read.tsfile("test.tsfile").write.tsfile("out")

	```
	
* **Example 6(using options to write)**

	```scala
	import cn.edu.thu.tsfile._
	import org.apache.spark.sql.SparkSession
	import scala.collection.mutable

	val spark = SparkSession.builder().master("local").getOrCreate()
		
	val df = spark.read.option("delta_object_name", "root.device.turbine").tsfile("test.tsfile")
	    
	df.write.option("delta_object_name", "root.device.turbine").tsfile("out")
	```


### 6.2 spark-shell

```
$ bin/spark-shell --jars tsfile-spark-connector-0.1.0.jar,tsfile-0.1.0.jar

scala> sql("CREATE TEMPORARY TABLE TsFile_table USING cn.edu.thu.tsfile OPTIONS (path \"hdfs://localhost:9000/test1.tsfile\")")

scala> val df = sql("select * from TsFile_table")

scala> df.write.tsfile("out")

```