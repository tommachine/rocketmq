package com.cafebabe.rocketmq.simple;

import com.cafebabe.rocketmq.constant.RocketmqConstant;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class SimpleConsumer {
    public static void main(String[] args) throws MQClientException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(RocketmqConstant.SIMPLE_CONSUMER_GROUP);
        consumer.setNamesrvAddr(RocketmqConstant.NAMESRV_ADDR);
        //设置广播消息，这里不用
        //consumer.setMessageModel(MessageModel.BROADCASTING);
        consumer.subscribe(RocketmqConstant.SIMPLE_TOPIC, RocketmqConstant.SIMPLE_TAG);
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
                for (MessageExt msg : msgs) {
                    System.out.println("消费者接收到消息：" + new String(msg.getBody(), StandardCharsets.UTF_8));
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        consumer.start();
    }
}
