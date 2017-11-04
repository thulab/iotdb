package cn.edu.tsinghua.kafka_iotdbDemo;

import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
//读取CSV文件的库
import java.io.BufferedReader; 
import java.io.File;
import java.io.FileReader; 

public class KafkaProducer {
	private final Producer<String, String> producer;
	public final static String TOPIC = "test";

	private KafkaProducer() {
		//接下来是Producer的配置
		Properties props = new Properties();
		// 此处配置的是kafka的端
		props.put("metadata.broker.list", "127.0.0.1:9092");
		props.put("zk.connect", "127.0.0.1:2181");  

		// 配置value的序列化类
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		// 配置key的序列化类
		props.put("key.serializer.class", "kafka.serializer.StringEncoder");

		props.put("request.required.acks", "-1");

		//Producer实例化
		producer = new Producer<String, String>(new ProducerConfig(props));
	}

	void produce() {
		//按行读取文件内容
		try {
			File csv = new File("F:\\workspace\\java\\myeclipse2015\\KafkaToIotdbDemo\\data.csv"); // CSV文件路径
			BufferedReader reader = new BufferedReader(new FileReader(csv));// 打开文件
			String line = null;
			int messageNum = 1;
			while ((line = reader.readLine()) != null) {
				String key = String.valueOf(messageNum);
				String data = line;
				producer.send(new KeyedMessage<String, String>(TOPIC, key, data));
				System.out.println(data);
				messageNum++;
			}
			reader.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		new KafkaProducer().produce();
	}
}
