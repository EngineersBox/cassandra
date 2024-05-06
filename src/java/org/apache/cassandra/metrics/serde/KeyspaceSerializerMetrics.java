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

package org.apache.cassandra.metrics.serde;

import java.time.Duration;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Timer;
import org.apache.cassandra.metrics.CassandraMetricsRegistry;
import org.apache.cassandra.metrics.MetricNameFactory;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

public class KeyspaceSerializerMetrics  {

    public final EnumMap<SerializerType, Timer> timers;

    private final MetricNameFactory nameFactory;
    private final String namePrefix;

    public KeyspaceSerializerMetrics(final MetricNameFactory nameFactory, final String namePrefix) {
        this.nameFactory = nameFactory;
        this.namePrefix = namePrefix;
        this.timers = new EnumMap<>(SerializerType.class);
        Arrays.stream(SerializerType.values()).forEach(this::register);
    }

    private CassandraMetricsRegistry.MetricName createMetricName(final SerializerType type) {
        return this.nameFactory.createMetricName(this.namePrefix + type.metricName() + "SerializerRate");
    }

    private void register(final SerializerType type) {
        final CassandraMetricsRegistry.MetricName name = createMetricName(type);
        this.timers.put(
            type,
            Metrics.timer(name)
        );
    }

    public void update(final SerializerType type,
                       final Duration duration) {
        final Timer timer = this.timers.get(type);
        if (timer == null) {
            throw new IllegalArgumentException("Unregistered serializer type: " + type.name());
        }
        timer.update(duration);
    }

    public void update(final SerializerType type,
                       final long duration,
                       final TimeUnit unit) {
        final Timer timer = this.timers.get(type);
        if (timer == null) {
            throw new IllegalArgumentException("Unregistered serializer type: " + type.name());
        }
        timer.update(duration, unit);
    }

    public void release() {
        Arrays.stream(SerializerType.values())
              .map(this::createMetricName)
              .forEach(Metrics::remove);
    }

}
