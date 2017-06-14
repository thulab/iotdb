# cn.edu.thu.tsfile-spark-connector

将一个或多个TsFile展示成SparkSQL中的一张表。允许指定单个目录，或使用通配符匹配多个目录。如果是多个TsFile，schema将保留各个TsFile中sensor的并集。


## 示例

src/test/scala/cn.edu.thu.cn.edu.thu.tsfile.spark.TSFileSuit


## 路径指定方式


basefolder/key=1/file1.cn.edu.thu.tsfile

basefolder/key=2/file2.cn.edu.thu.tsfile
指定basefolder为path，会在表中多加一列key，值为1或2。

如：
path=basefolder


如果使用通配符指定，将不会当做partiton

如：
path=basefolder/\*/\*.cn.edu.thu.tsfile


basefolder/file1.cn.edu.thu.tsfile
basefolder/file2.cn.edu.thu.tsfile

指定basefolder会将多个tsfile的schema合并，保留sensor的并集

如：
path=basefolder


## 版本需求

The versions required for Spark and Java are as follow:

| Spark Version | Scala Version | Java Version |
| ------------- | ------------- | ------------ |
| `2.0+`        | `2.11`        | `1.8`        |



## 数据类型转化

This library uses the following mapping the data type from TsFile to SparkSQL:

| TsFile 		   | SparkSQL|
| --------------| -------------- |
| INT32       		   | IntegerType    |
| INT64       		   | LongType       |
| FLOAT       		   | FloatType      |
| DOUBLE      		   | DoubleType     |


#### TsFile Schema -> SparkSQL Table Structure

The set of time-series data in section "Time-series Data" is used here to illustrate the mapping from TsFile Schema to SparkSQL Table Stucture.

<center>
<table style="text-align:center">
	<tr><th colspan="6">delta\_object:turbine1</th></tr>
	<tr><th colspan="2">sensor_1</th><th colspan="2">sensor_2</th><th colspan="2">sensor_3</th></tr>
	<tr><th>time</th><th>value</td><th>time</th><th>value</td><th>time</th><th>value</td>
	<tr><td>1</td><td>1.2</td><td>1</td><td>20</td><td>2</td><td>50</td></tr>
	<tr><td>3</td><td>1.4</td><td>2</td><td>20</td><td>4</td><td>51</td></tr>
	<tr><td>5</td><td>1.1</td><td>3</td><td>21</td><td>6</td><td>52</td></tr>
	<tr><td>7</td><td>1.8</td><td>4</td><td>20</td><td>8</td><td>53</td></tr>
</table>
<span>A set of time-series data</span>
</center>

There are two reserved columns in Spark SQL Table:

- `time` : Timestamp, LongType
- `delta_object` : Delta_object ID, StringType

The SparkSQL Table Structure is as follow:

<center>
	<table style="text-align:center">
	<tr><th>time(LongType)</th><th> delta\_object(StringType)</th><th>sensor_1(FloatType)</th><th>sensor_2(IntType)</th><th>sensor_3(IntType)</th></tr>
	<tr><td>1</td><td> turbine1 </td><td>1.2</td><td>20</td><td>null</td></tr>
	<tr><td>2</td><td> turbine1 </td><td>null</td><td>20</td><td>50</td></tr>
	<tr><td>3</td><td> turbine1 </td><td>1.4</td><td>21</td><td>null</td></tr>
	<tr><td>4</td><td> turbine1 </td><td>null</td><td>20</td><td>51</td></tr>
	<tr><td>5</td><td> turbine1 </td><td>1.1</td><td>null</td><td>null</td></tr>
	<tr><td>6</td><td> turbine1 </td><td>null</td><td>null</td><td>52</td></tr>
	<tr><td>7</td><td> turbine1 </td><td>1.8</td><td>null</td><td>null</td></tr>
	<tr><td>8</td><td> turbine1 </td><td>null</td><td>null</td><td>53</td></tr>
	</table>

</center>

#### Examples

##### Scala API

* **Example 1**

	```scala
	// import this library and Spark
	import cn.edu.thu.tsfile._
	import org.apache.spark.sql.SparkSession

	val spark = SparkSession.builder().master("local").getOrCreate()

	//read data in TsFile and create a table
	val df = spark.read.tsfile("test.ts")
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
	      .format("cn.edu.thu.cn.edu.thu.tsfile.spark")
	      .load("test.ts")


	df.filter("sensor_1 > 1.2").show()

	```

* **Example 3**

	```scala
	import cn.edu.thu.tsfile._
    import org.apache.spark.sql.SparkSession
   	
	val spark = SparkSession.builder().master("local").getOrCreate()

	//create a table in SparkSQL and build relation with a TsFile
	spark.sql("create temporary view TsFile using cn.edu.thu.cn.edu.thu.tsfile.spark options(path = \"test.ts\")")

	spark.sql("select * from TsFile where sensor_1 > 1.2").show()

	```

##### spark-shell

可以将项目打包在 `spark-shell`中使用。

```
mvn clean scala:compile compile package -DskipTests
```

包所在位置：
target/tsfile-spark-connector-0.1.0.jar


```
$ bin/spark-shell --jars target/tsfile-spark-connector-0.1.0.jar,tsfile-0.1.0.jar

scala> sql("CREATE TEMPORARY TABLE TsFile_table USING cn.edu.thu.cn.edu.thu.tsfile OPTIONS (path \"hdfs://localhost:9000/test1.tsfile\")")

scala> sql("select * from TsFile_table").show()
```