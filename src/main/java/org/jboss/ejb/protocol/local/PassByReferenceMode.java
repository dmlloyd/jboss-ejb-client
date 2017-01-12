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

import java.util.EnumSet;

/**
 * An enumeration of possible pass-by-reference modes.
 *
 * @author <a href="mailto:david.lloyd@redhat.com">David M. Lloyd</a>
 */
public enum PassByReferenceMode {
    /**
     * Always pass by reference without testing the values for visibility by the target class loader.  This yields the
     * best performance but is dependent on a completely correct class loader arrangement.
     */
    ALWAYS,
    /**
     * Attempt to only pass by reference "safely", that is, when it is likely that the classes are compatible between
     * source and target class loaders.
     */
    SAFE,
    /**
     * Never pass by reference; instead, always copy values even if the class loaders of the objects are compatible.
     * Note that even in this case, certain immutable object classes are still passed by reference.
     */
    NEVER,
    ;

    private static final int fullSize = values().length;

    /**
     * Determine whether the given set is fully populated (or "full"), meaning it contains all possible values.
     *
     * @param set the set
     * @return {@code true} if the set is full, {@code false} otherwise
     */
    public static boolean isFull(final EnumSet<PassByReferenceMode> set) {
        return set != null && set.size() == fullSize;
    }

    /**
     * Determine whether this instance is equal to one of the given instances.
     *
     * @param v1 the first instance
     * @return {@code true} if one of the instances matches this one, {@code false} otherwise
     */
    public boolean in(final PassByReferenceMode v1) {
        return this == v1;
    }

    /**
     * Determine whether this instance is equal to one of the given instances.
     *
     * @param v1 the first instance
     * @param v2 the second instance
     * @return {@code true} if one of the instances matches this one, {@code false} otherwise
     */
    public boolean in(final PassByReferenceMode v1, final PassByReferenceMode v2) {
        return this == v1 || this == v2;
    }

    /**
     * Determine whether this instance is equal to one of the given instances.
     *
     * @param v1 the first instance
     * @param v2 the second instance
     * @param v3 the third instance
     * @return {@code true} if one of the instances matches this one, {@code false} otherwise
     */
    public boolean in(final PassByReferenceMode v1, final PassByReferenceMode v2, final PassByReferenceMode v3) {
        return this == v1 || this == v2 || this == v3;
    }

    /**
     * Determine whether this instance is equal to one of the given instances.
     *
     * @param values the possible values
     * @return {@code true} if one of the instances matches this one, {@code false} otherwise
     */
    public boolean in(final PassByReferenceMode... values) {
        if (values != null) for (PassByReferenceMode value : values) {
            if (this == value) return true;
        }
        return false;
    }
}
