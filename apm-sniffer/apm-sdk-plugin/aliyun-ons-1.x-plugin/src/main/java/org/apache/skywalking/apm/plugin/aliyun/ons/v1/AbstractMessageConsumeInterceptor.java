/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */


package org.apache.skywalking.apm.plugin.aliyun.ons.v1;

import com.aliyun.openservices.ons.api.PropertyKeyConst;
import com.aliyun.openservices.ons.api.impl.rocketmq.ONSClientAbstract;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.consumer.listener.MessageListenerOrderly;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.message.MessageExt;
import org.apache.skywalking.apm.agent.core.context.CarrierItem;
import org.apache.skywalking.apm.agent.core.context.ContextCarrier;
import org.apache.skywalking.apm.agent.core.context.ContextManager;
import org.apache.skywalking.apm.agent.core.context.trace.AbstractSpan;
import org.apache.skywalking.apm.agent.core.context.trace.SpanLayer;
import org.apache.skywalking.apm.agent.core.plugin.interceptor.enhance.EnhancedInstance;
import org.apache.skywalking.apm.agent.core.plugin.interceptor.enhance.InstanceMethodsAroundInterceptor;
import org.apache.skywalking.apm.agent.core.plugin.interceptor.enhance.MethodInterceptResult;
import org.apache.skywalking.apm.network.trace.component.ComponentsDefine;
import org.apache.skywalking.apm.plugin.aliyun.ons.v1.context.MQTags;
import org.apache.skywalking.apm.util.MachineInfo;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Properties;

/**
 * {@link AbstractMessageConsumeInterceptor} create entry span when the <code>consumeMessage</code> in the {@link
 * org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently} and {@link
 * org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly} class.
 *
 * @author zhangxin
 */
public abstract class AbstractMessageConsumeInterceptor implements InstanceMethodsAroundInterceptor {

    public static final String CONSUMER_OPERATION_NAME_PREFIX = "ONS/";

    @Override
    public final void beforeMethod(EnhancedInstance objInst, Method method, Object[] allArguments,
        Class<?>[] argumentsTypes,
        MethodInterceptResult result) throws Throwable {
        List<MessageExt> msgs = (List<MessageExt>)allArguments[0];

        ContextCarrier contextCarrier = getContextCarrierFromMessage(msgs.get(0));
        AbstractSpan span = ContextManager.createEntrySpan(CONSUMER_OPERATION_NAME_PREFIX + msgs.get(0).getTopic() + "/Consumer", contextCarrier);

        span.setComponent(ComponentsDefine.ROCKET_MQ_CONSUMER);
        SpanLayer.asMQ(span);
        MQTags.MQ_HOST.set(span, MachineInfo.getHostDesc());
        Properties consumerProperties = this.obtainConsumerProperties(objInst);
        if (consumerProperties != null) {
            MQTags.MQ_GROUP.set(span, consumerProperties.getProperty(PropertyKeyConst.GROUP_ID, "UNKNOWN"));
            MQTags.MQ_MESSAGE_MODEL.set(span, consumerProperties.getProperty(PropertyKeyConst.MessageModel, "UNKNOWN"));
        } else {
            MQTags.MQ_GROUP.set(span, "UNKNOWN");
            MQTags.MQ_MESSAGE_MODEL.set(span, "UNKNOWN");
        }
        MQTags.MQ_CONSUMER_TYPE.set(span, this.obtainConsumerType(objInst));

        // 处理其它消息的 ContextCarrier ，进行关联
        for (int i = 1; i < msgs.size(); i++) {
            ContextManager.extract(getContextCarrierFromMessage(msgs.get(i)));
        }
    }

    @Override public final void handleMethodException(EnhancedInstance objInst, Method method, Object[] allArguments,
        Class<?>[] argumentsTypes, Throwable t) {
        ContextManager.activeSpan().errorOccurred().log(t);
    }

    private ContextCarrier getContextCarrierFromMessage(MessageExt message) {
        ContextCarrier contextCarrier = new ContextCarrier();

        CarrierItem next = contextCarrier.items();
        while (next.hasNext()) {
            next = next.next();
            next.setHeadValue(message.getUserProperty(next.getHeadKey()));
        }

        return contextCarrier;
    }

//    private String obtainConsumerGroup(EnhancedInstance objInst) {
//        try {
//            // TODO 芋艿，优化下 field 不要重复获取
//            Field field = objInst.getClass().getDeclaredField("consumerGroup");
//            field.setAccessible(true);
//            return field.get(objInst).toString();
//        } catch (Throwable e) {
//            return "ERROR：" + e.getMessage();
//        }
//    }
//
    private String obtainConsumerType(EnhancedInstance objInst) {
        if (objInst instanceof MessageListenerOrderly) {
            return "Orderly";
        }
        if (objInst instanceof MessageListenerConcurrently) {
            return "Concurrently";
        }
        return "UNKNOWN：" + objInst.getClass().getSimpleName();
    }

    private Properties obtainConsumerProperties(EnhancedInstance objInst) {
        try {
            // 获得外部类，该外部类的父类是 TODO 芋艿，后续优化下。
            Field consumerField = objInst.getClass().getDeclaredField("this$0");
            consumerField.setAccessible(true);
            ONSClientAbstract consumer = (ONSClientAbstract) consumerField.get(objInst);
            // 获得 Properties 属性
            Field propertiesField = ONSClientAbstract.class.getDeclaredField("properties");
            propertiesField.setAccessible(true);
            return (Properties) propertiesField.get(consumer);
        } catch (Throwable e) {
            return null;
        }
    }

}
