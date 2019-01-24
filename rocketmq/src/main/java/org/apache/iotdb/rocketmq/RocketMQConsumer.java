package org.apache.iotdb.rocketmq;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.*;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

/**
 * Created by litianan on 2019/1/21.
 */
public class ConsumerDemo {
    public static void main(String[] args) throws MQClientException {
        // Instantiate with specified consumer group name.
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("Consumer1");
        // Specify name server addresses.
        consumer.setNamesrvAddr("localhost:9876");

        // Subscribe one more more topics to consume.
        consumer.subscribe("TopicTest", "*");
        /**
         * Setting Consumer to start first from the head of the queue or from the tail of the queue
         * If not for the first time, then continue to consume according to the position of last consumption.
         */
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);

        // Register callback to execute on arrival of messages fetched from brokers.
        consumer.registerMessageListener(new MessageListenerOrderly() {

            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
                System.out.printf("%s Receive New Messages: %s %n", Thread.currentThread().getName(), new String(msgs.get(0).getBody()));
//                System.out.printf("%s Receive New Messages: %s %s %n", Thread.currentThread().getName(), new String(msgs.get(0).getBody()), msgs);
                return ConsumeOrderlyStatus.SUCCESS;
            }
        });

        //Launch the consumer instance.
        consumer.start();
    }
}
