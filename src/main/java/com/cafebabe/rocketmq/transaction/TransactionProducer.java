package com.cafebabe.rocketmq.transaction;

import com.cafebabe.rocketmq.constant.RocketmqConstant;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

import java.nio.charset.StandardCharsets;
import java.util.Scanner;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author cafebabe on 2021/8/8 13:01
 */
public class TransactionProducer {

    public static void main(String[] args) throws MQClientException {
        TransactionMQProducer producer = new TransactionMQProducer(RocketmqConstant.TRANSACTION_PRODUCER_GROUP);
        producer.setNamesrvAddr(RocketmqConstant.NAMESRV_ADDR);

        TransactionListener transactionListener = new TransactionListenerImpl();
        //设置事务监听器
        producer.setTransactionListener(transactionListener);
        ExecutorService executorService = new ThreadPoolExecutor(4, 8, 5, TimeUnit.MINUTES, new ArrayBlockingQueue<>(1000));
        producer.setExecutorService(executorService);


        producer.start();

        Scanner scanner = new Scanner(System.in);
        System.out.println("请输入消息的messageKey:");
        while (scanner.hasNext()) {
            int messageKey = scanner.nextInt();
            scanner.nextLine();
            System.out.println("请输入事务消息：");
            String nextLine = scanner.nextLine();

            Message message = new Message(RocketmqConstant.TRANSACTION_TOPIC, RocketmqConstant.TRANSACTION_TAG, nextLine.getBytes(StandardCharsets.UTF_8));
            message.setKeys(String.valueOf(messageKey));
            producer.sendMessageInTransaction(message, messageKey);
            System.out.println("请输入消息的messageKey:");
        }

        producer.shutdown();
    }
}
