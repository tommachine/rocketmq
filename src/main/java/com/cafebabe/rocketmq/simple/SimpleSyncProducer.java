package com.cafebabe.rocketmq.simple;

import com.cafebabe.rocketmq.constant.RocketmqConstant;
import com.cafebabe.rocketmq.utils.RocketmqUtil;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.nio.charset.StandardCharsets;
import java.util.Scanner;

public class SimpleSyncProducer {
    public static void main(String[] args) throws MQClientException, MQBrokerException, RemotingException, InterruptedException {
        DefaultMQProducer producer = RocketmqUtil.getDefaultMqProducer();
        //同步发送失败重试次数
        producer.setRetryTimesWhenSendFailed(3);
        //设置超时时间为3s
        producer.setSendMsgTimeout(3000);
        //设置默认的队列数量，及分区数量
        producer.setDefaultTopicQueueNums(4);

        producer.start();

        Scanner scanner = new Scanner(System.in);
        while (scanner.hasNext()) {
            String next = scanner.nextLine();
            Message msg = new Message(RocketmqConstant.SIMPLE_TOPIC, RocketmqConstant.SIMPLE_TAG, next.getBytes(StandardCharsets.UTF_8));
            SendResult sendResult = producer.send(msg);
            System.out.println("发送结果为：" + sendResult);
        }
        producer.shutdown();
    }
}
