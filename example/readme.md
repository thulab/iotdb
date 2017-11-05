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
  For details, please refer to http://kafka.apache.org/081/documentation.html#quickstart
```

## Run KafkaProducer.java

```
  The program is to send data from localhost to Kafka clusters.
  Firstly, you have to change the parameter of TOPIC to what you create：(for example : "test")
  > public final static String TOPIC = "test";
  The default format of date is "device,timestamp,value ". (for example : "sensor1,2017/10/24 19:30:00,60")
  Then you need to create a .CSV file in your localhost, which contains the data to be transmitted.Then set the parameter of Filename：(for example : "c:\\data.csv")
  > private final static String fileName = "c:\\data.csv";
  Finally, run KafkaProducer.java
```

## Run KafkaConsumer.java

```
  The program is to consume data from Kafka.
  You can set the parameter of threadsNum to make sure the number of consumer threads:(for example: "5")
  > private final static int threadsNum = 5;
```

### Notice 
  If you want to use multiple consumers, please make sure that the number of topic'spartition you create is more than 1.

## Run SendDataToIotdb.java

```
  The program is to upload data to IoTDB.
  You can set the parameter of threadsNum to make sure the number of consumer threads:(for example: "5")
  > private final static int threadsNum = 5;
  Then you change the insert EQL according to your localhost's data.
  Finally, run SendDataToIotdb.java
```
