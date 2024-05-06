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

import java.util.Arrays;
import java.util.EnumMap;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

import com.codahale.metrics.Timer;
import org.apache.cassandra.metrics.CassandraMetricsRegistry;
import org.apache.cassandra.metrics.MetricNameFactory;
import org.apache.cassandra.metrics.TableMetrics;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

public class TableSerializerMetrics
{

    public final EnumMap<SerializerType, TableMetrics.TableTimer> timers;

    private final MetricNameFactory nameFactory;
    private final String namePrefix;
    private final KeyspaceSerializerMetrics keyspaceSerializerMetrics;
    private final BiFunction<String, Timer, TableMetrics.TableTimer> timerProvider;

    public TableSerializerMetrics(final MetricNameFactory nameFactory,
                                  final String namePrefix,
                                  final KeyspaceSerializerMetrics keyspaceSerializerMetrics,
                                  final BiFunction<String, Timer, TableMetrics.TableTimer> timerProvider) {
        this.nameFactory = nameFactory;
        this.namePrefix = namePrefix;
        this.timers = new EnumMap<>(SerializerType.class);
        this.keyspaceSerializerMetrics = keyspaceSerializerMetrics;
        this.timerProvider = timerProvider;
        Arrays.stream(SerializerType.values()).forEach(this::register);

    }

    private CassandraMetricsRegistry.MetricName createMetricName(final SerializerType type) {
        return this.nameFactory.createMetricName(this.namePrefix + type.metricName() + "SerializerRate");
    }

    private void register(final SerializerType type) {
        final CassandraMetricsRegistry.MetricName name = createMetricName(type);
        this.timers.put(
            type,
            this.timerProvider.apply(
                name.getMetricName(),
                this.keyspaceSerializerMetrics.timers.get(type)
            )
        );
    }

    public void update(final SerializerType type,
                       final long duration,
                       final TimeUnit unit) {
        final TableMetrics.TableTimer timer = this.timers.get(type);
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
