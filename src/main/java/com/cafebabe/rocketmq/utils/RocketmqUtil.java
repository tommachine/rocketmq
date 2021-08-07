package com.cafebabe.rocketmq.utils;

import com.cafebabe.rocketmq.constant.RocketmqConstant;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.producer.DefaultMQProducer;

/**
 * @author cafebabe on 2021/8/7
 */
public class RocketmqUtil {

    /**
     * @return
     */
    public static DefaultMQProducer getDefaultMqProducer() {
        return getDefaultMqProducer(null, null);
    }

    public static DefaultMQProducer getDefaultMqProducer(String producerGroup, String nameSrv) {
        DefaultMQProducer defaultMQProducer;
        if (StringUtils.isNotBlank(producerGroup)) {
            defaultMQProducer = new DefaultMQProducer(producerGroup);
        }
        defaultMQProducer = new DefaultMQProducer(RocketmqConstant.SIMPLE_PRODUCER_GROUP);
        defaultMQProducer.setNamesrvAddr(StringUtils.isBlank(nameSrv) ? RocketmqConstant.NAMESRV_ADDR : nameSrv);
        return defaultMQProducer;
    }

    public static DefaultMQProducer getDefaultMqProducer(String nameSrv) {
        return getDefaultMqProducer(null, nameSrv);
    }


}
