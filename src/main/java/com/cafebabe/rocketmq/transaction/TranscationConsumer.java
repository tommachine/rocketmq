package com.cafebabe.rocketmq.transaction;

import com.cafebabe.rocketmq.constant.RocketmqConstant;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * @author cafebabe on 2021/8/8 13:27
 */
public class TranscationConsumer {
    public static void main(String[] args) throws MQClientException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(RocketmqConstant.TRANSACTION_CONSUMER_GROUP);

        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.setNamesrvAddr(RocketmqConstant.NAMESRV_ADDR);
        consumer.subscribe(RocketmqConstant.TRANSACTION_TOPIC, RocketmqConstant.TRANSACTION_TAG);

        consumer.registerMessageListener(new MessageListenerConcurrently() {

            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                for (MessageExt msg : msgs) {
                    System.out.println("messageKey:[" + msg.getKeys() + "]message :[" + new String(msg.getBody(), StandardCharsets.UTF_8) + "]");
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        consumer.start();
    }
}
