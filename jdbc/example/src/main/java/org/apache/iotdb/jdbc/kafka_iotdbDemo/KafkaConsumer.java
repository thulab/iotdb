/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/**
 * The class is to show how to get data from kafka through multi-threads.
 * The data is sent by class KafkaProducer.
 */
package org.apache.iotdb.jdbc.kafka_iotdbDemo;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import kafka.serializer.StringDecoder;
import kafka.utils.VerifiableProperties;
import org.apache.iotdb.jdbc.jdbcDemo.*;

public class KafkaConsumer {

  private final static int threadsNum = 5; // consumer threads
  private final ConsumerConnector consumer;
  private ExecutorService executor;

  private KafkaConsumer() {
    // Consumer configuration
    Properties props = new Properties();

    // zookeeper configuration
    props.put("zookeeper.connect", "127.0.0.1:2181");

    props.put("group.id", "consumeGroup");

    // ZooKeeper session timeout
    props.put("zookeeper.session.timeout.ms", "400");

    // a ZooKeeper 'follower' can be behind the master before an error
    // occurs
    props.put("zookeeper.sync.time.ms", "200");
    props.put("rebalance.max.retries", "5");

    // Backoff time between retries during rebalance
    props.put("rebalance.backoff.ms", "1200");

    // how often updates to the consumed offsets are written to ZooKeeper
    props.put("auto.commit.interval.ms", "1000");

    // What to do when there is no initial offset in ZooKeeper or if an
    // offset is out of range:
    // * smallest : automatically reset the offset to the smallest offset
    props.put("auto.offset.reset", "smallest");

    // serializer class
    props.put("serializer.class", "kafka.serializer.StringEncoder");

    // consumer instance
    ConsumerConfig config = new ConsumerConfig(props);
    consumer = kafka.consumer.Consumer.createJavaConsumerConnector(config);
  }

  public static void main(String[] args) throws Exception {
    new KafkaConsumer().consume();
  }

  void consume() throws Exception {
    // specify the number of consumer threads
    Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
    topicCountMap.put(KafkaProducer.TOPIC, new Integer(threadsNum));

    // specify data decoder
    StringDecoder keyDecoder = new StringDecoder(new VerifiableProperties());
    StringDecoder valueDecoder = new StringDecoder(new VerifiableProperties());

    Map<String, List<KafkaStream<String, String>>> consumerMap = consumer
        .createMessageStreams(topicCountMap, keyDecoder,
            valueDecoder); // 3 Strings are TOPIC, Key, Value

    // acquire data
    List<KafkaStream<String, String>> streams = consumerMap.get(KafkaProducer.TOPIC);

    // multi-threaded consume
    executor = Executors.newFixedThreadPool(threadsNum);    //create a thread pool
    for (final KafkaStream<String, String> stream : streams) {
      executor.submit(new ConsumerThread(stream));        // run thread
    }
  }
}

class ConsumerThread implements Runnable {

  private KafkaStream<String, String> stream;
  private SendDataToIotdb sendDataToIotdb;

  public ConsumerThread(KafkaStream<String, String> stream) throws Exception {
    this.stream = stream;
    // establish JDBC connection of IoTDB
    sendDataToIotdb = new SendDataToIotdb();
    sendDataToIotdb.connectToIotdb();
  }

  public void run() {
    ConsumerIterator<String, String> it = stream.iterator();
    while (it.hasNext()) {
      MessageAndMetadata<String, String> consumerIterator = it.next();
      String uploadMessage = consumerIterator.message();
      System.out.println(Thread.currentThread().getName()
          + " from partiton[" + consumerIterator.partition() + "]: "
          + uploadMessage);
      try {
        sendDataToIotdb.writeData(uploadMessage); // upload data to the IoTDB database

      } catch (Exception ex) {
        System.out.println("SQLException: " + ex.getMessage());
      }
    }
  }
}