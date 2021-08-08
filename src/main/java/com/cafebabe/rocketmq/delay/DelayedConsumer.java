package com.cafebabe.rocketmq.delay;

import com.cafebabe.rocketmq.constant.RocketmqConstant;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;

import java.nio.charset.StandardCharsets;
import java.time.LocalTime;
import java.util.List;

/**
 * @author cafebabe on 2021/8/8 11:23
 */
public class DelayedConsumer {

    public static void main(String[] args) throws MQClientException, InterruptedException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(RocketmqConstant.SIMPLE_CONSUMER_GROUP);
        consumer.setNamesrvAddr(RocketmqConstant.NAMESRV_ADDR);

        consumer.subscribe(RocketmqConstant.DELAYDE_TOPIC, RocketmqConstant.DELAYED_TAG);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                LocalTime localTime = LocalTime.now();
                for (MessageExt msg : msgs) {
                    System.out.println(localTime.toString() + ":" + new String(msg.getBody(), StandardCharsets.UTF_8));
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        consumer.start();

    }
}
