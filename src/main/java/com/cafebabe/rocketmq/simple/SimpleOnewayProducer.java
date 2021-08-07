package com.cafebabe.rocketmq.simple;

import com.cafebabe.rocketmq.constant.RocketmqConstant;
import com.cafebabe.rocketmq.utils.RocketmqUtil;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.nio.charset.StandardCharsets;
import java.util.Scanner;

public class SimpleOnewayProducer {
    public static void main(String[] args) throws MQClientException, RemotingException, InterruptedException {
        DefaultMQProducer producer = RocketmqUtil.getDefaultMqProducer();
        producer.start();
        Scanner scanner = new Scanner(System.in);
        while (scanner.hasNext()) {
            String nextLine = scanner.nextLine();
            Message message = new Message(RocketmqConstant.SIMPLE_TOPIC, RocketmqConstant.SIMPLE_TAG, nextLine.getBytes(StandardCharsets.UTF_8));
            producer.sendOneway(message);
        }
        producer.shutdown();
    }
}
