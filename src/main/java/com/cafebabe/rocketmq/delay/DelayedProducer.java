package com.cafebabe.rocketmq.delay;

import com.cafebabe.rocketmq.constant.RocketmqConstant;
import com.cafebabe.rocketmq.utils.RocketmqUtil;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.nio.charset.StandardCharsets;
import java.time.LocalTime;
import java.util.Scanner;

/**
 * @author cafebabe on 2021/8/8 11:23
 */
public class DelayedProducer {

    public static void main(String[] args) throws MQBrokerException, RemotingException, InterruptedException, MQClientException {
        DefaultMQProducer producer = RocketmqUtil.getDefaultMqProducer();

        producer.setNamesrvAddr(RocketmqConstant.NAMESRV_ADDR);

        producer.setSendMsgTimeout(5000);

        producer.setRetryTimesWhenSendFailed(3);

        producer.start();

        Scanner scanner = new Scanner(System.in);

        //messageDelayLevel = 1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h 1d
        System.out.println("请输入延迟级别(1~19)：");
        while (scanner.hasNext()) {
            int delayTime = scanner.nextInt();
            scanner.nextLine();
            System.out.println("请输入延迟消息体");
            String next = scanner.nextLine();

            Message message = new Message(RocketmqConstant.DELAYDE_TOPIC, RocketmqConstant.DELAYED_TAG, next.getBytes(StandardCharsets.UTF_8));
            message.setDelayTimeLevel(delayTime);
            SendResult sendResult = producer.send(message);
            LocalTime localTime = LocalTime.now();
            System.out.println(localTime.toString() +":"+sendResult);
            System.out.println("请输入延迟级别(1~19)：");
        }

        producer.shutdown();

    }
}
