package com.cafebabe.rocketmq.simple;

import com.cafebabe.rocketmq.constant.RocketmqConstant;
import com.cafebabe.rocketmq.utils.RocketmqUtil;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.nio.charset.StandardCharsets;
import java.util.Scanner;

public class SimpleAsyncProducer {
    public static void main(String[] args) throws MQClientException, RemotingException, InterruptedException {
        DefaultMQProducer producer = RocketmqUtil.getDefaultMqProducer();
        //异步发送失败数量
        producer.setRetryTimesWhenSendAsyncFailed(0);
        //设置超时时间为3s
        producer.setSendMsgTimeout(3000);
        //设置默认的队列数量，即分区数量
        producer.setDefaultTopicQueueNums(4);
        Scanner scanner = new Scanner(System.in);
        while (scanner.hasNext()) {
            String next = scanner.nextLine();
            Message message = new Message(RocketmqConstant.SIMPLE_TOPIC, RocketmqConstant.SIMPLE_TAG, next.getBytes(StandardCharsets.UTF_8));
            producer.send(message, new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    System.out.println("异步接受到发送结果为：" + sendResult);
                }

                @Override
                public void onException(Throwable throwable) {
                    System.out.println(throwable.getCause());
                }
            });
        }
        producer.shutdown();
    }
}
