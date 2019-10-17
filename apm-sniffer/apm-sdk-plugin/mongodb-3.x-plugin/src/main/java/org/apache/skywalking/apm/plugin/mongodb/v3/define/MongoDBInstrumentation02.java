package org.apache.skywalking.apm.plugin.mongodb.v3.define;

import net.bytebuddy.description.method.MethodDescription;
import net.bytebuddy.matcher.ElementMatcher;
import org.apache.skywalking.apm.agent.core.plugin.interceptor.StaticMethodsInterceptPoint;
import org.apache.skywalking.apm.agent.core.plugin.interceptor.enhance.ClassStaticMethodsEnhancePluginDefine;
import org.apache.skywalking.apm.agent.core.plugin.match.ClassMatch;
import org.apache.skywalking.apm.agent.core.plugin.match.NameMatch;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.takesArguments;

public class MongoDBInstrumentation02 extends ClassStaticMethodsEnhancePluginDefine {

    private static final String ENHANCE_CLASS = "com.mongodb.operation.CommandOperationHelper";

    private static final String COMMAND_OPERATION_HELPER_INTERCEPTOR = "org.apache.skywalking.apm.plugin.mongodb.v3.MongoDBMethodInterceptor02";

    @Override
    protected ClassMatch enhanceClass() {
        return NameMatch.byName(ENHANCE_CLASS);
    }

    @Override
    public StaticMethodsInterceptPoint[] getStaticMethodsInterceptPoints() {
        return new StaticMethodsInterceptPoint[]{
                new StaticMethodsInterceptPoint() {
                    @Override
                    public ElementMatcher<MethodDescription> getMethodsMatcher() {
                        return named("executeWrappedCommandProtocol").and(takesArguments(8));
                    }

                    @Override
                    public String getMethodsInterceptor() {
                        return COMMAND_OPERATION_HELPER_INTERCEPTOR;
                    }

                    @Override
                    public boolean isOverrideArgs() {
                        return false;
                    }
                }
        };
    }

}
