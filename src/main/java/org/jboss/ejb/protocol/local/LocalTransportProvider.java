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

import org.jboss.ejb.client.EJBReceiver;
import org.jboss.ejb.client.EJBTransportProvider;
import org.jboss.ejb.server.Association;
import org.wildfly.common.Assert;

/**
 * A local (same-JVM) transport provider which couples directly to an {@link Association}.
 *
 * @author <a href="mailto:david.lloyd@redhat.com">David M. Lloyd</a>
 */
public final class LocalTransportProvider implements EJBTransportProvider {
    private final Association association;
    private final PassByReferenceMode passByReferenceMode;

    /**
     * Construct a new instance.
     *
     * @param association the association to use (must not be {@code null})
     * @param passByReferenceMode the pass-by-reference mode (must not be {@code null})
     */
    public LocalTransportProvider(final Association association, final PassByReferenceMode passByReferenceMode) {
        Assert.checkNotNullParam("association", association);
        Assert.checkNotNullParam("passByReferenceMode", passByReferenceMode);
        this.association = association;
        this.passByReferenceMode = passByReferenceMode;
    }

    public boolean supportsProtocol(final String uriScheme) {
        return uriScheme != null && uriScheme.equals("local");
    }

    public EJBReceiver getReceiver(final String uriScheme) throws IllegalArgumentException {
        if (uriScheme == null || ! uriScheme.equals("local")) {
            throw new IllegalArgumentException("Unsupported EJB receiver protocol " + uriScheme);
        }
        return new LocalEJBReceiver(association, passByReferenceMode);
    }
}
