# Function
```
The example is to show how to send data from localhost to IoTDB through Kafka.
```
# Usage
## Dependencies with Maven

```
<dependencies>
    <dependency>
    	<groupId>org.apache.kafka</groupId>
    	<artifactId>kafka_2.10</artifactId>
    	<version>0.8.2.0</version>
	</dependency>
	<dependency>
      <groupId>cn.edu.tsinghua</groupId>
      <artifactId>iotdb-jdbc</artifactId>
      <version>0.1.2</version>
    </dependency>
</dependencies>
```

## Launch the servers

```
  Before you run the program, make sure you have launched the servers of Kafka and IoTDB.
  For details, please refer to [](http://kafka.apache.org/081/documentation.html#quickstart)
```
## How to install in local maven repository

> mvn clean install -Dmaven.test.skip=true
