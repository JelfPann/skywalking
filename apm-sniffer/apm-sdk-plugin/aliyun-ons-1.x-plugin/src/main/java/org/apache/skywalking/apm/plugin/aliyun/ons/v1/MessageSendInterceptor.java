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

import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.impl.CommunicationMode;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.producer.SendResult;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.message.Message;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.protocol.header.SendMessageRequestHeader;
import org.apache.skywalking.apm.agent.core.context.CarrierItem;
import org.apache.skywalking.apm.agent.core.context.ContextCarrier;
import org.apache.skywalking.apm.agent.core.context.ContextManager;
import org.apache.skywalking.apm.agent.core.context.tag.Tags;
import org.apache.skywalking.apm.agent.core.context.trace.AbstractSpan;
import org.apache.skywalking.apm.agent.core.context.trace.SpanLayer;
import org.apache.skywalking.apm.agent.core.plugin.interceptor.enhance.EnhancedInstance;
import org.apache.skywalking.apm.agent.core.plugin.interceptor.enhance.InstanceMethodsAroundInterceptor;
import org.apache.skywalking.apm.agent.core.plugin.interceptor.enhance.MethodInterceptResult;
import org.apache.skywalking.apm.network.trace.component.ComponentsDefine;
import org.apache.skywalking.apm.plugin.aliyun.ons.v1.context.SendCallBackEnhanceInfo;
import org.apache.skywalking.apm.util.StringUtil;

import java.lang.reflect.Method;

import static com.aliyun.openservices.shade.com.alibaba.rocketmq.common.message.MessageDecoder.NAME_VALUE_SEPARATOR;
import static com.aliyun.openservices.shade.com.alibaba.rocketmq.common.message.MessageDecoder.PROPERTY_SEPARATOR;

/**
 * {@link MessageSendInterceptor} create exit span when the method {@link org.apache.rocketmq.client.impl.MQClientAPIImpl#sendMessage(String,
 * String, Message, org.apache.rocketmq.common.protocol.header.SendMessageRequestHeader, long,
 * org.apache.rocketmq.client.impl.CommunicationMode, org.apache.rocketmq.client.producer.SendCallback,
 * org.apache.rocketmq.client.impl.producer.TopicPublishInfo, org.apache.rocketmq.client.impl.factory.MQClientInstance,
 * int, org.apache.rocketmq.client.hook.SendMessageContext, org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl)}
 * execute.
 *
 * @author zhang xin
 */
public class MessageSendInterceptor implements InstanceMethodsAroundInterceptor {

    public static final String ASYNC_SEND_OPERATION_NAME_PREFIX = "ONS/";

    @Override
    public void beforeMethod(EnhancedInstance objInst, Method method, Object[] allArguments, Class<?>[] argumentsTypes,
        MethodInterceptResult result) throws Throwable {
        Message message = (Message)allArguments[2];
        ContextCarrier contextCarrier = new ContextCarrier();
        String namingServiceAddress = String.valueOf(objInst.getSkyWalkingDynamicField());
        AbstractSpan span = ContextManager.createExitSpan(buildOperationName(message.getTopic()), contextCarrier, namingServiceAddress);
        span.setComponent(ComponentsDefine.ROCKET_MQ_PRODUCER);
        Tags.MQ_BROKER.set(span, (String)allArguments[0]);
        Tags.MQ_TOPIC.set(span, message.getTopic());
        // TODO 芋艿 client messageId
        // TODO 芋艿 发送类型
        // TODO 芋艿 queueId
        // TODO 芋艿 消息内容
        SpanLayer.asMQ(span);

        SendMessageRequestHeader requestHeader = (SendMessageRequestHeader)allArguments[3];
        StringBuilder properties = new StringBuilder(requestHeader.getProperties());
        CarrierItem next = contextCarrier.items();
        while (next.hasNext()) {
            next = next.next();
            if (!StringUtil.isEmpty(next.getHeadValue())) {
                properties.append(next.getHeadKey());
                properties.append(NAME_VALUE_SEPARATOR);
                properties.append(next.getHeadValue());
                properties.append(PROPERTY_SEPARATOR);
            }
        }
        requestHeader.setProperties(properties.toString());

        // 处理 callback 的情况
        if (allArguments[6] != null) {
            ((EnhancedInstance)allArguments[6]).setSkyWalkingDynamicField(new SendCallBackEnhanceInfo(message.getTopic(), ContextManager.capture()));
        }
    }

    @Override
    public Object afterMethod(EnhancedInstance objInst, Method method, Object[] allArguments, Class<?>[] argumentsTypes,
        Object ret) throws Throwable {
        CommunicationMode communicationMode = (CommunicationMode) allArguments[5];
        // 同步的情况下，设置发送状态
        if (CommunicationMode.SYNC.equals(communicationMode)) {
            // 发送结果
            SendResult result = (SendResult) ret;
            // 获得当前 Span
            AbstractSpan span = ContextManager.activeSpan();
            Tags.STATUS_CODE.set(span, result.getSendStatus().name());
            // TODO server messageId
        }
        ContextManager.stopSpan();
        return ret;
    }

    @Override
    public void handleMethodException(EnhancedInstance objInst, Method method, Object[] allArguments,
        Class<?>[] argumentsTypes, Throwable t) {
        ContextManager.activeSpan().errorOccurred().log(t);
    }

    private String buildOperationName(String topicName) {
        return ASYNC_SEND_OPERATION_NAME_PREFIX + topicName + "/Producer";
    }

}
