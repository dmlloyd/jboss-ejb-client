/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2017 Red Hat, Inc., and individual contributors
 * as indicated by the @author tags.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.jboss.ejb.protocol.local;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.security.PrivilegedAction;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

import javax.ejb.AsyncResult;
import javax.ejb.EJBException;
import javax.ejb.NoSuchEJBException;
import javax.transaction.Transaction;

import org.jboss.ejb._private.Logs;
import org.jboss.ejb.client.EJBClientInvocationContext;
import org.jboss.ejb.client.EJBIdentifier;
import org.jboss.ejb.client.EJBLocator;
import org.jboss.ejb.client.EJBMethodLocator;
import org.jboss.ejb.client.EJBReceiver;
import org.jboss.ejb.client.EJBReceiverInvocationContext;
import org.jboss.ejb.client.SessionID;
import org.jboss.ejb.client.StatefulEJBLocator;
import org.jboss.ejb.client.StatelessEJBLocator;
import org.jboss.ejb.server.Association;
import org.jboss.ejb.server.InvocationRequest;
import org.jboss.ejb.server.Request;
import org.jboss.ejb.server.SessionOpenRequest;
import org.jboss.marshalling.cloner.ClassCloner;
import org.jboss.marshalling.cloner.ClassLoaderClassCloner;
import org.jboss.marshalling.cloner.ClonerConfiguration;
import org.jboss.marshalling.cloner.ObjectCloner;
import org.jboss.marshalling.cloner.ObjectCloners;
import org.wildfly.common.Assert;
import org.wildfly.common.annotation.NotNull;
import org.wildfly.common.function.ExceptionSupplier;
import org.wildfly.security.manager.WildFlySecurityManager;
import org.wildfly.transaction.client.ContextTransactionManager;

/**
 * @author <a href="mailto:david.lloyd@redhat.com">David M. Lloyd</a>
 */
final class LocalEJBReceiver extends EJBReceiver {
    private static final Object[] EMPTY_OBJECT_ARRAY = new Object[0];

    private final Association association;
    private final PassByReferenceMode passByReferenceMode;

    LocalEJBReceiver(Association association, final PassByReferenceMode passByReferenceMode) {
        this.association = association;
        this.passByReferenceMode = passByReferenceMode;
    }

    protected void processInvocation(final EJBReceiverInvocationContext receiverContext) throws Exception {
        final EJBClientInvocationContext invocation = receiverContext.getClientInvocationContext();
        final EJBLocator<?> locator = invocation.getLocator();
        final Transaction transaction = receiverContext.getClientInvocationContext().getTransaction();

        final ClonerConfiguration paramConfig = new ClonerConfiguration();
        final ClassLoader classLoader = association.mapClassLoader(locator.getAppName(), locator.getModuleName());
        paramConfig.setClassCloner(new ClassLoaderClassCloner(classLoader));

        final ObjectCloner parameterCloner = createCloner(paramConfig);

        final Object[] invocationParameters = invocation.getParameters();
        final Object[] parameters;
        if (invocationParameters == null || invocationParameters.length == 0) {
            parameters = EMPTY_OBJECT_ARRAY;
        } else if (passByReferenceMode == PassByReferenceMode.ALWAYS) {
            parameters = invocation.getParameters();
        } else if (passByReferenceMode == PassByReferenceMode.SAFE) {
            // TODO: we can far better than this.  See https://issues.jboss.org/browse/JBMAR-192
            final Class<?>[] invokedParamTypes = invocation.getInvokedMethod().getParameterTypes();
            parameters = new Object[invocation.getParameters().length];
            for (int i = 0; i < parameters.length; ++i) {
                if (invokedParamTypes[i].isPrimitive() ||
                    invocationParameters[i] == null ||
                    ((Class<?>) parameterCloner.clone(invokedParamTypes)).isInstance(invocationParameters[i])
                ) {
                    parameters[i] = invocationParameters[i];
                } else {
                    parameters[i] = parameterCloner.clone(invocationParameters[i]);
                }
            }
        } else {
            assert passByReferenceMode == PassByReferenceMode.NEVER;
            parameters = (Object[]) parameterCloner.clone(invocationParameters);
        }

        final ClonerConfiguration config = new ClonerConfiguration();
        config.setClassCloner(new LocalInvocationClassCloner(WildFlySecurityManager.getClassLoaderPrivileged(invocation.getInvokedProxy().getClass())));

        // TODO: we should only clone the response object when it is necessary when in SAFE mode.
        final ObjectCloner resultCloner = passByReferenceMode == PassByReferenceMode.ALWAYS ? ObjectCloner.IDENTITY : createCloner(config);

        association.receiveInvocationRequest(LocalInvocation.newInstance(
            transaction,
            receiverContext,
            parameters,
            locator,
            resultCloner
        ));
    }

    protected <T> StatefulEJBLocator<T> createSession(final StatelessEJBLocator<T> statelessLocator) throws Exception {
        final ContextTransactionManager transactionManager = ContextTransactionManager.getInstance();
        final Transaction transaction = transactionManager.suspend();
        try {
            final LocalSessionRequest request = new LocalSessionRequest(statelessLocator, transaction);
            association.receiveSessionOpenRequest(request);
            Object result = request.getResult();
            if (result instanceof SessionID) {
                return statelessLocator.withSessionId((SessionID) result);
            } else if (result instanceof Exception) {
                throw (Exception) result;
            } else {
                throw Assert.unreachableCode();
            }
        } finally {
            if (transaction != null) {
                transactionManager.resume(transaction);
            }
        }
    }

    private static ObjectCloner createCloner(final ClonerConfiguration paramConfig) {
        ObjectCloner parameterCloner;
        if (WildFlySecurityManager.isChecking()) {
            parameterCloner = WildFlySecurityManager.doUnchecked((PrivilegedAction<ObjectCloner>) () -> ObjectCloners.getSerializingObjectClonerFactory().createCloner(paramConfig));
        } else {
            parameterCloner = ObjectCloners.getSerializingObjectClonerFactory().createCloner(paramConfig);
        }
        return parameterCloner;
    }

    abstract static class LocalRequest implements Request {
        private final Transaction transaction;

        protected LocalRequest(final Transaction transaction) {
            this.transaction = transaction;
        }

        public Executor getRequestExecutor() {
            return Runnable::run;
        }

        public String getProtocol() {
            return "local";
        }

        public boolean hasTransaction() {
            return transaction != null;
        }

        public Transaction getTransaction() {
            return transaction;
        }
    }

    static final class LocalInvocation<T> extends LocalRequest implements InvocationRequest<T> {
        private final EJBReceiverInvocationContext context;
        private final Object[] parameters;
        private final EJBLocator<T> locator;
        private final ObjectCloner resultCloner;

        LocalInvocation(final Transaction transaction, final EJBReceiverInvocationContext context, final Object[] parameters, final EJBLocator<T> locator, final ObjectCloner resultCloner) {
            super(transaction);
            this.context = context;
            this.parameters = parameters;
            this.locator = locator;
            this.resultCloner = resultCloner;
        }

        @NotNull
        public Map<String, Object> getAttachments() {
            return context.getClientInvocationContext().getContextData();
        }

        @NotNull
        public EJBMethodLocator getMethodLocator() {
            return context.getClientInvocationContext().getMethodLocator();
        }

        @NotNull
        public Object[] getParameters() {
            return parameters;
        }

        @NotNull
        public EJBLocator<T> getEJBLocator() {
            return locator;
        }

        public boolean isBlockingCaller() {
            return context.getClientInvocationContext().isBlockingCaller();
        }

        public void execute(@NotNull final ExceptionSupplier<?, Exception> resultSupplier) {
            Assert.checkNotNullParam("resultSupplier", resultSupplier);
            context.resultReady(new EJBReceiverInvocationContext.ResultProducer() {
                public Object getResult() throws Exception {
                    try {
                        final Object result = resultSupplier.get();
                        if (result instanceof Future) {
                            return new AsyncResult<Object>(resultCloner.clone(((Future<?>) result).get()));
                        } else {
                            return result;
                        }
                    } catch (Exception e) {
                        throw (Exception) resultCloner.clone(e);
                    }
                }

                public void discardResult() {
                    // no operation
                }
            });
        }

        public void writeException(@NotNull final Exception exception) {
            try {
                context.resultReady(new EJBReceiverInvocationContext.ResultProducer.Failed((Exception) resultCloner.clone(exception)));
            } catch (IOException | ClassNotFoundException e) {
                context.resultReady(new EJBReceiverInvocationContext.ResultProducer.Failed(e));
            }
        }

        public void convertToStateful(@NotNull final SessionID sessionId) throws IllegalArgumentException, IllegalStateException {
            Assert.checkNotNullParam("sessionId", sessionId);
            context.getClientInvocationContext().setLocator(locator.asStateless().withSessionId(sessionId));
        }

        public void writeNoSuchMethod() {
            context.resultReady(new EJBReceiverInvocationContext.ResultProducer.Failed(new EJBException(Logs.MAIN.remoteMessageNoSuchMethod(getMethodLocator(), getEJBLocator()))));
        }

        public void writeSessionNotActive() {
            context.resultReady(new EJBReceiverInvocationContext.ResultProducer.Failed(new EJBException(Logs.MAIN.remoteMessageSessionNotActive(getMethodLocator(), getEJBLocator()))));
        }

        public void writeInvocationResult(final Object result) {
            context.resultReady(new EJBReceiverInvocationContext.ResultProducer.Immediate(result));
        }

        public void writeNoSuchEJB() {
            context.resultReady(new EJBReceiverInvocationContext.ResultProducer.Failed(new NoSuchEJBException(Logs.MAIN.remoteMessageNoSuchEJB(getEJBIdentifier()))));
        }

        public void writeCancelResponse() {
            context.requestCancelled();
        }

        public void writeNotStateful() {
            context.resultReady(new EJBReceiverInvocationContext.ResultProducer.Failed(new EJBException(Logs.MAIN.remoteMessageEJBNotStateful(getEJBIdentifier()))));
        }

        static <T> LocalInvocation<T> newInstance(final Transaction transaction, final EJBReceiverInvocationContext context, final Object[] parameters, final EJBLocator<T> locator, final ObjectCloner resultCloner) {
            return new LocalInvocation<T>(transaction, context, parameters, locator, resultCloner);
        }
    }

    static final class LocalInvocationClassCloner implements ClassCloner {

        private final ClassLoader destClassLoader;

        LocalInvocationClassCloner(final ClassLoader destClassLoader) {
            this.destClassLoader = destClassLoader;
        }

        public Class<?> clone(final Class<?> original) throws IOException, ClassNotFoundException {
            final String name = original.getName();
            if (name.startsWith("java.") || original.getClassLoader() == destClassLoader) {
                return original;
            } else try {
                return Class.forName(name, true, destClassLoader);
            } catch (ClassNotFoundException e) {
                return original;
            }
        }

        public Class<?> cloneProxy(final Class<?> proxyClass) throws IOException, ClassNotFoundException {
            final Class<?>[] origInterfaces = proxyClass.getInterfaces();
            final Class<?>[] interfaces = new Class[origInterfaces.length];
            for (int i = 0, origInterfacesLength = origInterfaces.length; i < origInterfacesLength; i++) {
                interfaces[i] = clone(origInterfaces[i]);
            }
            return Proxy.getProxyClass(destClassLoader, interfaces);
        }
    }

    static final class LocalSessionRequest extends LocalRequest implements SessionOpenRequest {
        private final StatelessEJBLocator<?> locator;
        private final AtomicReference<Object> resultRef = new AtomicReference<>();

        LocalSessionRequest(final StatelessEJBLocator<?> locator, final Transaction transaction) {
            super(transaction);
            this.locator = locator;
        }

        public boolean isBlockingCaller() {
            return true;
        }

        @NotNull
        public EJBIdentifier getEJBIdentifier() {
            return locator.getIdentifier();
        }

        public void writeNoSuchEJB() {
            writeException(new NoSuchEJBException(Logs.MAIN.remoteMessageNoSuchEJB(getEJBIdentifier())));
        }

        public void writeCancelResponse() {
            writeException(new CancellationException());
        }

        public void writeNotStateful() {
            writeException(new EJBException(Logs.MAIN.remoteMessageEJBNotStateful(getEJBIdentifier())));
        }

        public void writeException(@NotNull final Exception exception) {
            Assert.checkNotNullParam("exception", exception);
            resultRef.compareAndSet(null, exception);
        }

        public void convertToStateful(@NotNull final SessionID sessionId) throws IllegalArgumentException, IllegalStateException {
            Assert.checkNotNullParam("sessionId", sessionId);
            resultRef.compareAndSet(null, sessionId);
        }

        Object getResult() {
            return resultRef.get();
        }
    }
}
