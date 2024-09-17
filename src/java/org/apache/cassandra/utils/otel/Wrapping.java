/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.utils.otel;

import com.google.common.base.Function;

import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;

public class Wrapping
{

    private Wrapping() {
        throw new UnsupportedOperationException("Utility class");
    }

    public static <T, U> Function<T, U> function(final Context context,
                                                 final Function<T, U> function) {
        return (t) -> {
            final Scope ignored = context.makeCurrent();
            U var4;
            try {
                var4 = function.apply(t);
            } catch (Throwable var7) {
                if (ignored != null) {
                    try {
                        ignored.close();
                    } catch (Throwable var6) {
                        var7.addSuppressed(var6);
                    }
                }

                throw var7;
            }
            if (ignored != null) {
                ignored.close();
            }
            return var4;
        };
    }
}
