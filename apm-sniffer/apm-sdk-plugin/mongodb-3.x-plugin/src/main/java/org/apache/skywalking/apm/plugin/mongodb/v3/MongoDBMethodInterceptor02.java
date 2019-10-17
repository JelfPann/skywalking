package org.apache.skywalking.apm.plugin.mongodb.v3;

import com.mongodb.connection.Connection;
import org.apache.skywalking.apm.agent.core.conf.Config;
import org.apache.skywalking.apm.agent.core.context.ContextCarrier;
import org.apache.skywalking.apm.agent.core.context.ContextManager;
import org.apache.skywalking.apm.agent.core.context.tag.Tags;
import org.apache.skywalking.apm.agent.core.context.trace.AbstractSpan;
import org.apache.skywalking.apm.agent.core.context.trace.SpanLayer;
import org.apache.skywalking.apm.agent.core.plugin.interceptor.enhance.MethodInterceptResult;
import org.apache.skywalking.apm.agent.core.plugin.interceptor.enhance.StaticMethodsAroundInterceptor;
import org.apache.skywalking.apm.network.trace.component.ComponentsDefine;
import org.bson.BsonDocument;

import java.lang.reflect.Method;

public class MongoDBMethodInterceptor02 implements StaticMethodsAroundInterceptor {

    private static final String DB_TYPE = "MongoDB";

    private static final String MONGO_DB_OP_PREFIX = "MongoDB/";

    @Override
    public void beforeMethod(Class clazz, Method method, Object[] allArguments, Class<?>[] parameterTypes, MethodInterceptResult result) {
        String database = (String) allArguments[0];
        BsonDocument command = (BsonDocument) allArguments[1];
        Connection connection = (Connection) allArguments[4];
        String remotePeer = connection.getDescription().getServerAddress().toString();

        AbstractSpan span = ContextManager.createExitSpan(MONGO_DB_OP_PREFIX + allArguments[6].getClass().getName(), new ContextCarrier(), remotePeer);
        span.setComponent(ComponentsDefine.MONGO_DRIVER);
        Tags.DB_INSTANCE.set(span, database);
        Tags.DB_TYPE.set(span, DB_TYPE);
        SpanLayer.asDB(span);

        if (Config.Plugin.MongoDB.TRACE_PARAM || true) {
            Tags.DB_STATEMENT.set(span, command.toString());
        }
    }

    @Override
    public Object afterMethod(Class clazz, Method method, Object[] allArguments, Class<?>[] parameterTypes, Object ret) {
        ContextManager.stopSpan();
        return ret;
    }

    @Override
    public void handleMethodException(Class clazz, Method method, Object[] allArguments, Class<?>[] parameterTypes, Throwable t) {
        AbstractSpan activeSpan = ContextManager.activeSpan();
        activeSpan.errorOccurred();
        activeSpan.log(t);
    }

}
