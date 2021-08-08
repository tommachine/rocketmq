package com.cafebabe.rocketmq.ordered;

import com.cafebabe.rocketmq.constant.RocketmqConstant;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;

import java.time.LocalTime;
import java.util.List;

/**
 * @author cafebabe on 2021/8/8 10:51
 */
public class OrderedConsumer {

    public static void main(String[] args) throws MQClientException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(RocketmqConstant.SIMPLE_CONSUMER_GROUP);

        consumer.setNamesrvAddr(RocketmqConstant.NAMESRV_ADDR);

        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);

        consumer.subscribe(RocketmqConstant.ORDERED_TOPIC, RocketmqConstant.ORDERED_TAG);

        //单线程顺序消费
        consumer.registerMessageListener(new MessageListenerOrderly() {

            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
                LocalTime localTime = LocalTime.now();
                for (MessageExt msg : msgs) {
                    System.out.println(localTime.toString() + ":" + new String(msg.getBody()));
                }
                return ConsumeOrderlyStatus.SUCCESS;
            }
        });

        consumer.start();

    }
}
