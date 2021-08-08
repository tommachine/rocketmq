package com.cafebabe.rocketmq.ordered;

import com.cafebabe.rocketmq.constant.RocketmqConstant;
import com.cafebabe.rocketmq.utils.RocketmqUtil;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Random;
import java.util.Scanner;

/**
 * @author cafebabe on 2021/8/8 10:28
 */
public class OrderedProducer {

    public static void main(String[] args) throws MQClientException, MQBrokerException, RemotingException, InterruptedException {
        DefaultMQProducer producer = RocketmqUtil.getDefaultMqProducer();
        producer.setRetryTimesWhenSendFailed(3);
        producer.setSendMsgTimeout(5000);
        producer.start();

        Scanner scanner = new Scanner(System.in);
        System.out.println("请输入一个正整数的messageKey:");
        while (scanner.hasNext()) {
            int messageKey = scanner.nextInt();
            scanner.nextLine();
            System.out.println("请输入消息体：");
            String next = scanner.nextLine();
            //设置一个随机的messageKey
            Message msg = new Message(RocketmqConstant.ORDERED_TOPIC, RocketmqConstant.ORDERED_TAG, next.getBytes(StandardCharsets.UTF_8));
            msg.setKeys(String.valueOf(messageKey));

            SendResult send = producer.send(msg, new MessageQueueSelector() {
                @Override
                public MessageQueue select(List<MessageQueue> list, Message message, Object o) {
                    int queueSize = list.size();
                    int messageKey = (int) o;
                    int selectedMessageQueueIndex = messageKey % queueSize;
                    MessageQueue messageQueue = list.get(selectedMessageQueueIndex);
                    System.out.println(String.format("messageKey:[%s] 选择了队列id为[%s]的队列", messageKey, messageQueue.getQueueId()));
                    return messageQueue;
                }
            }, messageKey);
            System.out.println("发送状态：" + send);
            System.out.println("请输入一个正整数的messageKey:");
        }
        producer.shutdown();
    }
}
