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


package org.apache.skywalking.apm.plugin.mongodb.v3;

import com.mongodb.MongoNamespace;
import com.mongodb.ReadPreference;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.operation.ReadOperation;
import com.mongodb.operation.WriteOperation;
import org.apache.skywalking.apm.agent.core.conf.Config;
import org.apache.skywalking.apm.agent.core.context.ContextCarrier;
import org.apache.skywalking.apm.agent.core.context.ContextManager;
import org.apache.skywalking.apm.agent.core.context.tag.Tags;
import org.apache.skywalking.apm.agent.core.context.trace.AbstractSpan;
import org.apache.skywalking.apm.agent.core.context.trace.SpanLayer;
import org.apache.skywalking.apm.agent.core.plugin.interceptor.enhance.EnhancedInstance;
import org.apache.skywalking.apm.agent.core.plugin.interceptor.enhance.InstanceMethodsAroundInterceptor;
import org.apache.skywalking.apm.agent.core.plugin.interceptor.enhance.MethodInterceptResult;
import org.apache.skywalking.apm.network.trace.component.ComponentsDefine;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

/**
 * {@link MongoDBMethodInterceptor03} intercept method of {@link com.mongodb.Mongo#execute(ReadOperation, ReadPreference)}
 * or {@link com.mongodb.Mongo#execute(WriteOperation)}. record the mongoDB host, operation name and the key of the
 * operation.
 *
 * @author baiyang
 */
public class MongoDBMethodInterceptor03 implements InstanceMethodsAroundInterceptor {

    private static final String DB_TYPE = "MongoDB";

    private static final String MONGO_DB_OP_PREFIX = "MongoDB/";

    @Override public void beforeMethod(EnhancedInstance objInst, Method method, Object[] allArguments,
        Class<?>[] argumentsTypes, MethodInterceptResult result) throws Throwable {

        String database = this.obtainDatabase(objInst);
        String command = this.obtainCommand(objInst);
//        InternalConnection connection = (InternalConnection) allArguments[0];
//        String remotePeer = connection.getDescription().getServerAddress().toString();
        String remotePeer = this.obtainRemotePeer(allArguments[0]);

                // TODO url 的拼接，后面找下方案。实在不行，使用 collection name
        AbstractSpan span = ContextManager.createExitSpan(MONGO_DB_OP_PREFIX , new ContextCarrier(), remotePeer);
        span.setComponent(ComponentsDefine.MONGO_DRIVER);
        Tags.DB_INSTANCE.set(span, database);
        Tags.DB_TYPE.set(span, DB_TYPE);
        SpanLayer.asDB(span);

        if (Config.Plugin.MongoDB.TRACE_PARAM || true) {
            Tags.DB_STATEMENT.set(span, command);
        }
    }

    @Override
    public Object afterMethod(EnhancedInstance objInst, Method method, Object[] allArguments,
        Class<?>[] argumentsTypes, Object ret) throws Throwable {
        ContextManager.stopSpan();
        return ret;
    }

    @Override
    public void handleMethodException(EnhancedInstance objInst, Method method, Object[] allArguments,
        Class<?>[] argumentsTypes, Throwable t) {
        AbstractSpan activeSpan = ContextManager.activeSpan();
        activeSpan.errorOccurred();
        activeSpan.log(t);
    }

    private String obtainRemotePeer(Object connection) {
        try {
            Method method = connection.getClass().getDeclaredMethod("getDescription");
            method.setAccessible(true);
            ConnectionDescription description = (ConnectionDescription) method.invoke(connection);
            return description.getServerAddress().toString();
        } catch (Throwable e) {
            // TODO 需要打印一场
            return "UNKNOWN";
        }
    }

    private String obtainCommand(EnhancedInstance objInst) {
        try {
            // TODO 芋艿，优化下 field 不要重复获取
            Field field = objInst.getClass().getDeclaredField("command");
            field.setAccessible(true);
            return field.get(objInst).toString();
        } catch (Throwable e) {
            return "ERROR：" + e.getMessage();
        }
    }

    private String obtainDatabase(EnhancedInstance objInst) {
        try {
            Field field = objInst.getClass().getDeclaredField("namespace");
            field.setAccessible(true);
            return ((MongoNamespace) field.get(objInst)).getDatabaseName();
        } catch (Throwable e) {
            return "ERROR：" + e.getMessage();
        }
    }

}
