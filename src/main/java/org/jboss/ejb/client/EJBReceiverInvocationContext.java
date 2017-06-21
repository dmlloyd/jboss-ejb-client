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

import java.util.Arrays;

import javax.net.ssl.SSLContext;

import org.wildfly.common.Assert;
import org.wildfly.security.auth.client.AuthenticationConfiguration;

/**
 * The context used for an EJB receiver to return the result of an invocation.
 *
 * @author <a href="mailto:david.lloyd@redhat.com">David M. Lloyd</a>
 */
public final class EJBReceiverInvocationContext {

    private final EJBClientInvocationContext clientInvocationContext;

    EJBReceiverInvocationContext(final EJBClientInvocationContext clientInvocationContext) {
        this.clientInvocationContext = clientInvocationContext;
    }

    /**
     * Indicate that the invocation should proceed asynchronously, if it isn't already.
     */
    public void proceedAsynchronously() {
        clientInvocationContext.proceedAsynchronously();
    }

    /**
     * Indicate that the invocation result is ready.  The given producer should either return the final
     * invocation result or throw an appropriate exception.  Any unmarshalling is expected to be deferred until
     * the result producer is called, in order to offload the work on the invoking thread even in the presence of
     * asynchronous invocation.
     *
     * @param resultProducer the result producer
     */
    public void resultReady(ResultProducer resultProducer) {
        clientInvocationContext.resultReady(resultProducer);
    }

    /**
     * Indicate that the request was successfully cancelled and that no result is forthcoming.
     */
    public void requestCancelled() {
        clientInvocationContext.cancelled();
    }

    /**
     * Indicate that a request failed locally with the given exception cause.
     *
     * @param cause the failure cause (must not be {@code null})
     */
    public void requestFailed(Throwable cause) {
        Assert.checkNotNullParam("cause", cause);
        clientInvocationContext.failed(cause);
    }

    /**
     * Get the client invocation context associated with this receiver invocation context.
     *
     * @return the client invocation context
     */
    public EJBClientInvocationContext getClientInvocationContext() {
        return clientInvocationContext;
    }

    /**
     * Get the authentication configuration of the request.  The configuration may be associated with the proxy,
     * or it may have been inherited from the environment.
     *
     * @return the authentication configuration of the request (not {@code null})
     */
    public AuthenticationConfiguration getAuthenticationConfiguration() {
        return clientInvocationContext.getAuthenticationConfiguration();
    }

    /**
     * Get the SSL context of the request.  The SSL context may be associated with the proxy,
     * or it may have been inherited from the environment.
     *
     * @return the SSL context of the request, or {@code null} if no SSL context is configured
     */
    public SSLContext getSSLContext() {
        return clientInvocationContext.getSSLContext();
    }

    /**
     * A result producer for invocation.
     */
    public interface ResultProducer {

        /**
         * Get the result.
         *
         * @return the result
         * @throws Exception if the result could not be acquired or the operation failed
         */
        Object getResult() throws Exception;

        /**
         * Discard the result, indicating that it will not be used.
         */
        void discardResult();

        /**
         * A result producer for failure cases.
         */
        class Failed implements ResultProducer {
            private final Exception cause;

            /**
             * Construct a new instance.
             *
             * @param cause the failure cause
             */
            public Failed(final Exception cause) {
                this.cause = cause;
            }

            public Object getResult() throws Exception {
                final Exception cause = this.cause;
                final StackTraceElement[] stackTrace = new Throwable().getStackTrace();
                cause.setStackTrace(Arrays.copyOfRange(stackTrace, 1, stackTrace.length));
                throw cause;
            }

            public void discardResult() {
            }
        }

        class Immediate implements ResultProducer {
            private final Object result;

            public Immediate(final Object result) {
                this.result = result;
            }

            public Object getResult() throws Exception {
                return result;
            }

            public void discardResult() {
            }
        }
    }

}
