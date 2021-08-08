package com.cafebabe.rocketmq.transaction;

import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author cafebabe on 2021/8/8 13:10
 */
public class TransactionListenerImpl implements TransactionListener {


    /**
     * 模拟 若%3 == 0 则表示提交 %3 == 1 则表示回滚 %3 = 2 则表示回查
     *
     * @param msg
     * @param arg
     * @return
     */
    @Override
    public LocalTransactionState executeLocalTransaction(Message msg, Object arg) {
        int value = (int) arg;
        int status = value % 3;
        System.out.println("执行本地事务的消息为：" + new String(msg.getBody(), StandardCharsets.UTF_8) + "messageKey[" + value + "]");
        switch (status) {
            case 0:
                return LocalTransactionState.COMMIT_MESSAGE;
            case 1:
                return LocalTransactionState.ROLLBACK_MESSAGE;
            case 2:
                return LocalTransactionState.UNKNOW;
        }
        return LocalTransactionState.UNKNOW;
    }

    /**
     * 回查时这里给个提交状态，注意会查的时候不是立马会查，而是等待一段时间后会查
     * transactionTimeout 配置决定了会查的时间
     * 用户属性CHECK_IMMUNITY_TIME_IN_SECONDS 也可以在发送消息时设置在用户属性中这个参数优先于“transactionMsgTimeout”参数。
     *
     * @param msg
     * @return
     */
    @Override
    public LocalTransactionState checkLocalTransaction(MessageExt msg) {
        return LocalTransactionState.COMMIT_MESSAGE;
    }
}
