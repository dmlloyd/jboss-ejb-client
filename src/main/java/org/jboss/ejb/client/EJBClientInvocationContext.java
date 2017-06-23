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

package org.jboss.ejb.client;

import java.lang.reflect.Method;
import java.lang.reflect.UndeclaredThrowableException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import static java.lang.Math.max;
import static java.lang.Thread.holdsLock;

import javax.net.ssl.SSLContext;
import javax.transaction.Transaction;

import org.jboss.ejb._private.Logs;
import org.jboss.ejb.client.annotation.ClientTransactionPolicy;
import org.wildfly.common.Assert;
import org.wildfly.security.auth.client.AuthenticationConfiguration;

/**
 * An invocation context for EJB invocations from an EJB client
 *
 * @author <a href="mailto:david.lloyd@redhat.com">David M. Lloyd</a>
 * @author Jaikiran Pai
 */
public final class EJBClientInvocationContext extends AbstractInvocationContext {

    private static final Logs log = Logs.MAIN;

    public static final String PRIVATE_ATTACHMENTS_KEY = "org.jboss.ejb.client.invocation.attachments";

    // Contextual stuff
    private final EJBInvocationHandler<?> invocationHandler;

    // Invocation data
    private final Object invokedProxy;
    private final Object[] parameters;
    private final EJBProxyInformation.ProxyMethodInfo methodInfo;
    private final EJBReceiverInvocationContext receiverInvocationContext = new EJBReceiverInvocationContext(this);
    private final EJBClientContext.InterceptorList interceptorList;
    private final long startTime = System.nanoTime();
    private final long timeout;

    // Invocation state
    private final Object lock = new Object();
    private EJBReceiverInvocationContext.ResultProducer resultProducer;

    private volatile boolean cancelRequested;
    private boolean retryRequested;
    private State state = State.SENDING;
    private int remainingRetries;
    private Supplier<? extends Throwable> pendingFailure;
    private List<Supplier<? extends Throwable>> suppressedExceptions;
    private Object cachedResult;

    private int interceptorChainIndex;
    private boolean blockingCaller;
    private Transaction transaction;
    private AuthenticationConfiguration authenticationConfiguration;
    private SSLContext sslContext;

    EJBClientInvocationContext(final EJBInvocationHandler<?> invocationHandler, final EJBClientContext ejbClientContext, final Object invokedProxy, final Object[] parameters, final EJBProxyInformation.ProxyMethodInfo methodInfo, final int allowedRetries) {
        super(invocationHandler.getLocator(), ejbClientContext);
        this.invocationHandler = invocationHandler;
        this.invokedProxy = invokedProxy;
        this.parameters = parameters;
        this.methodInfo = methodInfo;
        long timeout = invocationHandler.getInvocationTimeout();
        if (timeout == -1) {
            timeout = ejbClientContext.getInvocationTimeout();
        }
        this.timeout = timeout;
        remainingRetries = allowedRetries;
        interceptorList = getClientContext().getInterceptors(getViewClass(), getInvokedMethod());
    }

    enum State {
        // waiting states

        SENDING(true),
        SENT(true),
        WAITING(true),

        // completion states

        READY(false),
        CONSUMING(false),
        DISCARDING(false),
        DONE(false),
        ;

        private final boolean waiting;

        State(final boolean waiting) {
            this.waiting = waiting;
        }

        boolean isWaiting() {
            return waiting;
        }
    }

    /**
     * Get a value attached to the proxy.
     *
     * @param key the attachment key
     * @param <T> the value type
     * @return the value, or {@code null} if there is none
     */
    public <T> T getProxyAttachment(AttachmentKey<T> key) {
        return invocationHandler.getAttachment(key);
    }

    /**
     * Remove a value attached to the proxy.
     *
     * @param key the attachment key
     * @param <T> the value type
     * @return the value, or {@code null} if there is none
     */
    public <T> T removeProxyAttachment(final AttachmentKey<T> key) {
        return invocationHandler.removeAttachment(key);
    }

    /**
     * Determine whether the method is marked client-asynchronous, meaning that invocation should be asynchronous regardless
     * of whether the server-side method is asynchronous.
     *
     * @return {@code true} if the method is marked client-asynchronous, {@code false} otherwise
     */
    public boolean isClientAsync() {
        return invocationHandler.isAsyncHandler() || methodInfo.isClientAsync();
    }

    /**
     * Determine whether the method is definitely synchronous, that is, it is not marked client-async, and the return
     * value of the method is not {@code void} or {@code Future<?>}.
     *
     * @return {@code true} if the method is definitely synchronous, {@code false} if the method may be asynchronous
     */
    public boolean isSynchronous() {
        return ! isClientAsync() && methodInfo.isSynchronous();
    }

    /**
     * Determine whether the method is marked idempotent, meaning that the method may be invoked more than one time with
     * no additional effect.
     *
     * @return {@code true} if the method is marked idempotent, {@code false} otherwise
     */
    public boolean isIdempotent() {
        return methodInfo.isIdempotent();
    }

    /**
     * Determine whether the method has an explicit transaction policy set.
     *
     * @return the transaction policy, if any, or {@code null} if none was explicitly set
     */
    public ClientTransactionPolicy getTransactionPolicy() {
        return methodInfo.getTransactionPolicy();
    }

    /**
     * Determine whether the request is expected to be compressed.
     *
     * @return {@code true} if the request is expected to be compressed, {@code false} otherwise
     */
    public boolean isCompressRequest() {
        return methodInfo.isCompressRequest();
    }

    /**
     * Determine whether the response is expected to be compressed.
     *
     * @return {@code true} if the response is expected to be compressed, {@code false} otherwise
     */
    public boolean isCompressResponse() {
        return methodInfo.isCompressResponse();
    }

    /**
     * Get the compression hint level.  If no compression hint is given, -1 is returned.
     *
     * @return the compression hint level, or -1 for no compression hint
     */
    public int getCompressionLevel() {
        return methodInfo.getCompressionLevel();
    }

    /**
     * Get the method type signature string, used to identify the method.
     *
     * @return the method signature string
     */
    public String getMethodSignatureString() {
        return methodInfo.getSignature();
    }

    /**
     * Get the EJB method locator.
     *
     * @return the EJB method locator
     */
    public EJBMethodLocator getMethodLocator() {
        return methodInfo.getMethodLocator();
    }

    /**
     * Determine whether this invocation is currently blocking the calling thread.
     *
     * @return {@code true} if the calling thread is being blocked; {@code false} otherwise
     */
    public boolean isBlockingCaller() {
        synchronized (lock) {
            return blockingCaller;
        }
    }

    /**
     * Establish whether this invocation is currently blocking the calling thread.
     *
     * @param blockingCaller {@code true} if the calling thread is being blocked; {@code false} otherwise
     */
    public void setBlockingCaller(final boolean blockingCaller) {
        synchronized (lock) {
            this.blockingCaller = blockingCaller;
        }
    }

    /**
     * Add a suppressed exception to the request.
     *
     * @param cause the suppressed exception (must not be {@code null})
     */
    public void addSuppressed(Throwable cause) {
        Assert.checkNotNullParam("cause", cause);
        synchronized (lock) {
            if (state == State.DONE) {
                return;
            }
            if (suppressedExceptions == null) {
                suppressedExceptions = new ArrayList<>();
            }
            suppressedExceptions.add(() -> cause);
        }
    }

    /**
     * Add a suppressed exception to the request.
     *
     * @param cause the suppressed exception (must not be {@code null})
     */
    public void addSuppressed(Supplier<? extends Throwable> cause) {
        Assert.checkNotNullParam("cause", cause);
        synchronized (lock) {
            if (state == State.DONE) {
                return;
            }
            if (suppressedExceptions == null) {
                suppressedExceptions = new ArrayList<>();
            }
            suppressedExceptions.add(cause);
        }
    }

    void sendRequestInitial() {
        assert checkState() == State.SENDING;
        for (;;) {
            assert interceptorChainIndex == 0;
            try {
                sendRequest();
                // back to the start of the chain; decide what to do next.
                synchronized (lock) {
                    assert state == State.SENT;
                    // from here we can go to: READY, or WAITING, or retry SENDING.
                    Supplier<? extends Throwable> pendingFailure = this.pendingFailure;
                    EJBReceiverInvocationContext.ResultProducer resultProducer = this.resultProducer;
                    if (resultProducer != null) {
                        // READY, even if we have a pending failure.
                        if (pendingFailure != null) {
                            addSuppressed(pendingFailure);
                            this.pendingFailure = null;
                        }
                        state = State.READY;
                        lock.notifyAll();
                        return;
                    }
                    // now see if we're retrying or returning.
                    if (pendingFailure != null) {
                        // either READY (with exception) or retry SENDING.
                        if (! retryRequested || remainingRetries == 0) {
                            // nobody wants retry, or there are none left; READY (with exception).
                            this.resultProducer = new ThrowableResult(pendingFailure);
                            this.pendingFailure = null;
                            // in case we've gone asynchronous
                            lock.notifyAll();
                            state = State.READY;
                            return;
                        } else {
                            // redo the loop
                            state = State.SENDING;
                            retryRequested = false;
                            remainingRetries --;
                            addSuppressed(pendingFailure);
                            continue;
                        }
                    }
                    state = State.WAITING;
                    lock.notifyAll();
                }
                // return to invocation handler
                return;
            } catch (Throwable t) {
                // back to the start of the chain; decide what to do next.
                synchronized (lock) {
                    assert state == State.SENT;
                    // from here we can go to: FAILED, READY, or retry SENDING.
                    Supplier<? extends Throwable> pendingFailure = this.pendingFailure;
                    EJBReceiverInvocationContext.ResultProducer resultProducer = this.resultProducer;
                    if (resultProducer != null) {
                        // READY, even if we have a pending failure.
                        if (pendingFailure != null) {
                            addSuppressed(t);
                            addSuppressed(pendingFailure);
                            this.pendingFailure = null;
                        }
                        state = State.READY;
                        lock.notifyAll();
                        return;
                    }
                    // FAILED, or retry SENDING.
                    if (! retryRequested || remainingRetries == 0) {
                        // nobody wants retry, or there are none left; go to FAILED
                        if (pendingFailure != null) {
                            addSuppressed(pendingFailure);
                        }
                        this.pendingFailure = () -> t;
                        state = State.READY;
                        // in case we've gone asynchronous
                        lock.notifyAll();
                        return;
                    }
                    // retry SENDING
                    setReceiver(null);
                    state = State.SENDING;
                    retryRequested = false;
                    remainingRetries --;
                }
                // record for later
                addSuppressed(t);
                // redo the loop
                //noinspection UnnecessaryContinue
                continue;
            }
        }
    }

    State checkState() {
        synchronized (lock) {
            return state;
        }
    }

    /**
     * Proceed with sending the request normally.
     *
     * @throws Exception if the request was not successfully sent
     */
    public void sendRequest() throws Exception {
        final EJBClientInterceptorInformation[] chain = interceptorList.getInformation();
        synchronized (lock) {
            if (state != State.SENDING) {
                throw Logs.MAIN.sendRequestCalledDuringWrongPhase();
            }
        }
        final int idx = interceptorChainIndex ++;
        try {
            if (chain.length == idx) {
                if (cancelRequested) {
                    synchronized (lock) {
                        state = State.SENT;
                    }
                    resultReady(CANCELLED);
                } else {
                    // End of the chain processing; deliver to receiver or throw an exception.
                    final URI destination = getDestination();
                    final EJBReceiver receiver = getClientContext().resolveReceiver(destination, getLocator());
                    setReceiver(receiver);
                    synchronized (lock) {
                        state = State.SENT;
                    }
                    receiver.processInvocation(receiverInvocationContext);
                }
            } else {
                chain[idx].getInterceptorInstance().handleInvocation(this);
                synchronized (lock) {
                    if (state != State.SENT) {
                        assert state == State.SENDING;
                        state = State.SENT;
                        throw Logs.INVOCATION.requestNotSent();
                    }
                }
            }
        } catch (Throwable t) {
            synchronized (lock) {
                state = State.SENT;
            }
            throw t;
        } finally {
            interceptorChainIndex--;
        }
        // return to enclosing interceptor
        return;
    }

    /**
     * Get the invocation result from this request.  The result is not actually acquired unless all interceptors
     * call this method.  Should only be called from {@link EJBClientInterceptor#handleInvocationResult(EJBClientInvocationContext)}.
     *
     * @return the invocation result
     * @throws Exception if the invocation did not succeed
     */
    public Object getResult() throws Exception {
        final EJBClientContext.InterceptorList list = getClientContext().getInterceptors(getViewClass(), getInvokedMethod());
        final EJBClientInterceptorInformation[] chain = list.getInformation();
        final EJBReceiverInvocationContext.ResultProducer resultProducer;
        Throwable fail = null;
        final int idx = this.interceptorChainIndex;
        synchronized (lock) {
            if (idx == 0) {
                while (state == State.CONSUMING) try {
                    lock.wait();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw Logs.MAIN.operationInterrupted();
                }
                if (state == State.DONE) {
                    Supplier<? extends Throwable> pendingFailure = this.pendingFailure;
                    if (pendingFailure != null) {
                        fail = pendingFailure.get();
                    } else {
                        return cachedResult;
                    }
                } else if (state != State.READY) {
                    throw Logs.MAIN.getResultCalledDuringWrongPhase();
                }
                state = State.CONSUMING;
            }
            resultProducer = this.resultProducer;
        }
        if (fail != null) try {
            throw fail;
        } catch (Exception | Error e) {
            throw e;
        } catch (Throwable t) {
            throw new UndeclaredThrowableException(t);
        }
        this.interceptorChainIndex = idx + 1;
        try {
            final Object result;
            try {
                if (idx == chain.length) {
                    result = resultProducer.getResult();
                } else {
                    result = chain[idx].getInterceptorInstance().handleInvocationResult(this);
                }
                if (idx == 0) {
                    synchronized (lock) {
                        state = State.DONE;
                        pendingFailure = null;
                        suppressedExceptions = null;
                        cachedResult = result;
                        lock.notifyAll();
                    }
                }
                return result;
            } catch (Throwable t) {
                if (idx == 0) {
                    synchronized (lock) {
                        state = State.DONE;
                        pendingFailure = () -> t;
                        List<Supplier<? extends Throwable>> suppressedExceptions = this.suppressedExceptions;
                        if (suppressedExceptions != null) {
                            this.suppressedExceptions = null;
                            for (Supplier<? extends Throwable> supplier : suppressedExceptions) {
                                try {
                                    t.addSuppressed(supplier.get());
                                } catch (Throwable ignored) {}
                            }
                        }
                        lock.notifyAll();
                    }
                }
                throw t;
            } finally {
                if (idx == 0) {
                    final Affinity weakAffinity = getWeakAffinity();
                    if (weakAffinity != null) {
                        invocationHandler.setWeakAffinity(weakAffinity);
                    }
                }
            }
        } finally {
            interceptorChainIndex = idx;
        }
    }

    /**
     * Discard the result from this request.  Should only be called from {@link EJBClientInterceptor#handleInvocationResult(EJBClientInvocationContext)}.
     *
     * @throws IllegalStateException if there is no result to discard
     */
    public void discardResult() throws IllegalStateException {
        resultReady(EJBClientInvocationContext.ONE_WAY);
    }

    void resultReady(EJBReceiverInvocationContext.ResultProducer resultProducer) {
        synchronized (lock) {
            if (state.isWaiting()) {
                this.resultProducer = resultProducer;
                if (state == State.WAITING) {
                    state = State.READY;
                    lock.notifyAll();
                }
                return;
            }
        }
        // for whatever reason, we don't care
        resultProducer.discardResult();
        return;
    }

    /**
     * Get the invoked proxy object.
     *
     * @return the invoked proxy
     */
    public Object getInvokedProxy() {
        return invokedProxy;
    }

    /**
     * Get the invoked proxy method.
     *
     * @return the invoked method
     */
    public Method getInvokedMethod() {
        return methodInfo.getMethod();
    }

    /**
     * Get the invocation method parameters.
     *
     * @return the invocation method parameters
     */
    public Object[] getParameters() {
        return parameters;
    }

    /**
     * Get the transaction associated with the invocation.  If there is no transaction (i.e. transactions should not
     * be propagated), {@code null} is returned.
     *
     * @return the transaction associated with the invocation, or {@code null} if no transaction should be propagated
     */
    public Transaction getTransaction() {
        return transaction;
    }

    /**
     * Set the transaction associated with the invocation.  If there is no transaction (i.e. transactions should not
     * be propagated), {@code null} should be set.
     *
     * @param transaction the transaction associated with the invocation, or {@code null} if no transaction should be
     *  propagated
     */
    public void setTransaction(final Transaction transaction) {
        this.transaction = transaction;
    }

    AuthenticationConfiguration getAuthenticationConfiguration() {
        return authenticationConfiguration;
    }

    void setAuthenticationConfiguration(final AuthenticationConfiguration authenticationConfiguration) {
        this.authenticationConfiguration = authenticationConfiguration;
    }

    SSLContext getSSLContext() {
        return sslContext;
    }

    void setSSLContext(final SSLContext sslContext) {
        this.sslContext = sslContext;
    }

    Future<?> getFutureResponse() {
        return new FutureResponse();
    }

    void proceedAsynchronously() {
        if (getInvokedMethod().getReturnType() == void.class) {
            resultReady(EJBReceiverInvocationContext.ResultProducer.NULL);
        }
    }

    /**
     * Wait to determine whether this invocation was cancelled.
     *
     * @return {@code true} if the invocation was cancelled; {@code false} if it completed or failed or the thread was
     *  interrupted
     */
    public boolean awaitCancellationResult() {
        assert ! holdsLock(lock);
        synchronized (lock) {
            for (;;) {
                if (resultProducer == CANCELLED) {
                    return true;
                } else if (! state.isWaiting()) {
                    return false;
                }
                try {
                    lock.wait();
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    return false;
                }
            }
        }
    }

    Object awaitResponse() throws Exception {
        assert !holdsLock(lock);
        boolean intr = false, timedOut = false;
        try {
            final Object lock = this.lock;
            final long timeout = this.timeout;
            synchronized (lock) {
                if (state == State.DONE) {
                    final Supplier<? extends Throwable> pendingFailure = this.pendingFailure;
                    if (pendingFailure != null) {
                        try {
                            throw pendingFailure.get();
                        } catch (Exception | Error e) {
                            throw e;
                        } catch (Throwable t) {
                            throw new UndeclaredThrowableException(t);
                        }
                    }
                    return cachedResult;
                }
                try {
                    if (timeout <= 0) {
                        // no timeout; lighter code path
                        while (state.isWaiting()) {
                            try {
                                lock.wait();
                            } catch (InterruptedException e) {
                                intr = true;
                            }
                        }
                    } else {
                        while (state.isWaiting()) {
                            long remaining = max(0L, timeout - max(0L, System.nanoTime() - startTime));
                            if (remaining == 0L) {
                                // timed out
                                timedOut = true;
                                break;
                            }
                            try {
                                lock.wait(remaining / 1_000_000L, (int) (remaining % 1_000_000L));
                            } catch (InterruptedException e) {
                                intr = true;
                            }
                        }
                    }
                } finally {
                    blockingCaller = false;
                }
            }
            if (timedOut) {
                resultReady(new ThrowableResult(() -> new TimeoutException("No invocation response received in " + timeout + " milliseconds")));
                final EJBReceiver receiver = getReceiver();
                if (receiver != null) receiver.cancelInvocation(receiverInvocationContext, false);
            }
            return getResult();
        } finally {
            if (intr) Thread.currentThread().interrupt();
        }
    }

    void setDiscardResult() {
        assert !holdsLock(lock);
        final EJBReceiverInvocationContext.ResultProducer resultProducer;
        synchronized (lock) {
            resultProducer = this.resultProducer;
            this.resultProducer = EJBReceiverInvocationContext.ResultProducer.NULL;
            // result is waiting, discard it
            if (state == State.WAITING) {
                state = State.DONE;
                lock.notifyAll();
            }
            // fall out of the lock to discard the old result (if any)
        }
        if (resultProducer != null) resultProducer.discardResult();
    }

    void cancelled() {
        resultReady(CANCELLED);
    }

    void failed(Exception exception, Executor retryExecutor) {
        synchronized (lock) {
            switch (state) {
                case CONSUMING:
                case DONE: {
                    // ignore
                    return;
                }
                case SENDING:
                case SENT: {
                    final Supplier<? extends Throwable> pendingFailure = this.pendingFailure;
                    if (pendingFailure != null) {
                        addSuppressed(pendingFailure);
                    }
                    this.pendingFailure = () -> exception;
                    return;
                }
                case READY: {
                    addSuppressed(exception);
                    return;
                }
                case WAITING: {
                    // async failure; decide what to do next.
                    // from here we can go to: READY (with failure), READY (ok), or retry SENDING.
                    Supplier<? extends Throwable> pendingFailure = this.pendingFailure;
                    EJBReceiverInvocationContext.ResultProducer resultProducer = this.resultProducer;
                    if (resultProducer != null) {
                        // READY (ok), even if we have a pending failure.
                        if (pendingFailure != null) {
                            addSuppressed(exception);
                            addSuppressed(pendingFailure);
                            this.pendingFailure = null;
                        }
                        state = State.READY;
                        lock.notifyAll();
                        return;
                    }
                    // READY (with failure), or retry SENDING.
                    if (! retryRequested || remainingRetries == 0) {
                        // nobody wants retry, or there are none left; go to READY (with failure)
                        if (pendingFailure != null) {
                            addSuppressed(pendingFailure);
                        }
                        this.pendingFailure = () -> exception;
                        state = State.READY;
                        // in case we've gone asynchronous
                        lock.notifyAll();
                        return;
                    }
                    // retry SENDING
                    setReceiver(null);
                    state = State.SENDING;
                    retryRequested = false;
                    remainingRetries --;

                    // record for later
                    addSuppressed(exception);
                    // redo the request
                    retryExecutor.execute(this::sendRequestInitial);
                    return;
                }
                default: {
                    throw Assert.impossibleSwitchCase(state);
                }
            }
        }
        // not reachable
    }

    final class FutureResponse implements Future<Object> {

        FutureResponse() {
        }

        public boolean cancel(final boolean mayInterruptIfRunning) {
            assert !holdsLock(lock);
            synchronized (lock) {
                if (! state.isWaiting()) {
                    return resultProducer == CANCELLED;
                }
                // at this point the task is running and we are allowed to interrupt it. So issue
                // a cancel request
                cancelRequested = true;
            }
            final EJBReceiver receiver = getReceiver();
            final boolean result = receiver != null && receiver.cancelInvocation(receiverInvocationContext, mayInterruptIfRunning);
            if (! result) {
                synchronized (lock) {
                    if (resultProducer == CANCELLED && ! state.isWaiting()) {
                        return true;
                    }
                }
            }
            return result;
        }

        public boolean isCancelled() {
            assert !holdsLock(lock);
            synchronized (lock) {
                return resultProducer == CANCELLED;
            }
        }

        public boolean isDone() {
            assert !holdsLock(lock);
            synchronized (lock) {
                return ! state.isWaiting();
            }
        }

        public Object get() throws InterruptedException, ExecutionException {
            assert !holdsLock(lock);
            EJBReceiverInvocationContext.ResultProducer resultProducer;
            final long timeout = EJBClientInvocationContext.this.timeout;
            boolean timedOut = false;
            synchronized (lock) {
                out: for (;;) {
                    switch (state) {
                        case SENDING:
                        case SENT:
                        case CONSUMING:
                        case WAITING: {
                            if (timeout <= 0) {
                                lock.wait();
                            } else {
                                long remaining = max(0L, timeout - max(0L, System.nanoTime() - startTime));
                                if (remaining == 0L) {
                                    // timed out
                                    timedOut = true;
                                    resultProducer = null;
                                    break out;
                                }
                                lock.wait(remaining / 1_000_000L, (int) (remaining % 1_000_000L));
                            }
                            break;
                        }
                        case READY: {
                            // Change state to consuming, but don't notify since nobody but us can act on it.
                            // Instead we'll notify after the result is consumed.
                            state = State.CONSUMING;
                            resultProducer = EJBClientInvocationContext.this.resultProducer;
                            EJBClientInvocationContext.this.resultProducer = null;
                            // we have to get the result, so break out of here.
                            break out;
                        }
                        case DONE: {
                            if (pendingFailure != null) {
                                throw log.remoteInvFailed(pendingFailure.get());
                            }
                            return cachedResult;
                        }
                        default:
                            throw new IllegalStateException();
                    }
                }
            }
            if (timedOut) {
                final TimeoutException timeoutException = new TimeoutException("No invocation response received in " + timeout + " milliseconds");
                resultReady(new ThrowableResult(() -> timeoutException));
                final EJBReceiver receiver = getReceiver();
                if (receiver != null) receiver.cancelInvocation(receiverInvocationContext, false);
                throw new ExecutionException(timeoutException);
            }
            return processResult(resultProducer);
        }

        public Object get(final long timeout, final TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            assert !holdsLock(lock);
            final long handlerInvTimeout = invocationHandler.getInvocationTimeout();
            final long invocationTimeout = handlerInvTimeout != -1 ? handlerInvTimeout : getClientContext().getInvocationTimeout();
            final long ourStart = System.nanoTime();
            if (unit.convert(max(0L, invocationTimeout - max(0L, ourStart - startTime)), TimeUnit.NANOSECONDS) <= timeout) {
                // the invocation will expire before we finish
                return get();
            }
            long remaining = unit.toNanos(timeout);
            final EJBReceiverInvocationContext.ResultProducer resultProducer;
            synchronized (lock) {
                out: for (;;) {
                    switch (state) {
                        case SENDING:
                        case SENT:
                        case CONSUMING:
                        case WAITING: {
                            if (remaining <= 0L) {
                                throw log.timedOut();
                            }
                            lock.wait(remaining / 1_000_000_000L, (int) (remaining % 1_000_000_000L));
                            remaining = unit.toNanos(timeout) - (System.nanoTime() - ourStart);
                            break;
                        }
                        case READY: {
                            // Change state to consuming, but don't notify since nobody but us can act on it.
                            // Instead we'll notify after the result is consumed.
                            state = State.CONSUMING;
                            resultProducer = EJBClientInvocationContext.this.resultProducer;
                            EJBClientInvocationContext.this.resultProducer = null;
                            // we have to get the result, so break out of here.
                            break out;
                        }
                        case DONE: {
                            if (pendingFailure != null) {
                                throw log.remoteInvFailed(pendingFailure.get());
                            }
                            return cachedResult;
                        }
                        default:
                            throw new IllegalStateException();
                    }
                }
            }
            return processResult(resultProducer);
        }

        private Object processResult(final EJBReceiverInvocationContext.ResultProducer resultProducer) throws ExecutionException {
            // extract the result from the producer.
            Object result;
            try {
                result = resultProducer.getResult();
            } catch (Throwable t) {
                synchronized (lock) {
                    assert state == State.CONSUMING;
                    state = State.DONE;
                    List<Supplier<? extends Throwable>> suppressedExceptions = EJBClientInvocationContext.this.suppressedExceptions;
                    if (suppressedExceptions != null) {
                        EJBClientInvocationContext.this.suppressedExceptions = null;
                        for (Supplier<? extends Throwable> supplier : suppressedExceptions) {
                            try {
                                t.addSuppressed(supplier.get());
                            } catch (Throwable ignored) {}
                        }
                    }
                    pendingFailure = () -> t;
                    lock.notifyAll();
                }
                throw log.remoteInvFailed(t);
            }
            synchronized (lock) {
                assert state == State.CONSUMING;
                state = State.DONE;
                pendingFailure = null;
                suppressedExceptions = null;
                cachedResult = result;
                lock.notifyAll();
            }
            return result;
        }
    }

    static final class ThrowableResult implements EJBReceiverInvocationContext.ResultProducer {
        private final Supplier<? extends Throwable> pendingFailure;

        ThrowableResult(final Supplier<? extends Throwable> pendingFailure) {
            this.pendingFailure = pendingFailure;
        }

        public Object getResult() throws Exception {
            try {
                throw pendingFailure.get();
            } catch (Exception | Error e) {
                throw e;
            } catch (Throwable t) {
                throw new UndeclaredThrowableException(t);
            }
        }

        public void discardResult() {
            // ignored
        }
    }

    static final ThrowableResult CANCELLED = new ThrowableResult(Logs.INVOCATION::requestCancelled);

    static final ThrowableResult ONE_WAY = new ThrowableResult(Logs.INVOCATION::oneWayInvocation);
}
