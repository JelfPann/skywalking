package org.apache.skywalking.apm.plugin.aliyun.ons.v1.context;

import org.apache.skywalking.apm.agent.core.context.tag.StringTag;

/**
 * MQ 拓展 Tag
 */
public class MQTags {

    public static final StringTag MQ_GROUP = new StringTag("mq.group");
    public static final StringTag MQ_TAG = new StringTag("mq.tag");
    public static final StringTag MQ_BODY = new StringTag("mq.body");
    public static final StringTag MQ_MESSAGE_ID = new StringTag("mq.msgId");
    public static final StringTag MQ_UNIQ_KEY = new StringTag("mq.uniqKey");
    /**
     * 消息模型，集群还是广播
     */
    public static final StringTag MQ_MESSAGE_MODEL = new StringTag("mq.model");
    /**
     * 消费者类型，顺序还是并发
     */
    public static final StringTag MQ_CONSUMER_TYPE = new StringTag("mq.consumerType");
    /**
     * 生产者/消费者，所在的 HOST 信息
     *
     * TODO 芋艿，因为 skywalking ui 不支持产生 span 的 host ，所以只能这样
     */
    public static final StringTag MQ_HOST = new StringTag("mq.host");

}
