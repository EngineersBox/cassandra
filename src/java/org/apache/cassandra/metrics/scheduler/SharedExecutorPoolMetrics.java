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

package org.apache.cassandra.metrics.scheduler;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Supplier;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import org.apache.cassandra.metrics.CassandraMetricsRegistry;
import org.apache.cassandra.metrics.MetricNameFactory;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

public class SharedExecutorPoolMetrics
{
    private final MetricNameFactory nameFactory = new SEPMetricNameFactory();
    private final Set<ReleasableMetric> releasable = new HashSet<>();
    public final Timer scheduleLatency;
    public final Timer maybeStartSpinningWorkerLatency;
    public final Counter spinningCounter;
    public final Counter descheduledCounter;
    public final Counter workersCounter;
    public final Counter executorsCounter;

    public SharedExecutorPoolMetrics(final Supplier<Integer> spinningSource,
                                     final Supplier<Integer> descheduledSource,
                                     final Supplier<Integer> workersSource,
                                     final Supplier<Integer> executorsSource) {
        this.scheduleLatency = timer("ScheduleLatency");
        this.maybeStartSpinningWorkerLatency = timer("MaybeStartSpinningWorkerLatency");

        this.spinningCounter = counterExternal("SpinningWorkers", spinningSource);
        this.descheduledCounter = counterExternal("DescheduledWorkers", descheduledSource);
        this.workersCounter = counterExternal("TotalWorkers", workersSource);
        this.executorsCounter = counterExternal("Executors", executorsSource);
    }

    private Timer timer(final String name) {
        final CassandraMetricsRegistry.MetricName metricName = this.nameFactory.createMetricName(name);
        register(metricName);
        return Metrics.timer(metricName);
    }

    private Counter counterExternal(final String name, Supplier<Integer> source) {
        final CassandraMetricsRegistry.MetricName metricName = this.nameFactory.createMetricName(name);
        register(metricName);
        return Metrics.counter(metricName, externalSourceCounter(source));
    }

    private MetricRegistry.MetricSupplier<Counter> externalSourceCounter(final Supplier<Integer> source) {
        return () -> new Counter() {

            @Override
            public long getCount() {
                return source.get();
            }

        };
    }

    private void register(final CassandraMetricsRegistry.MetricName name) {
        this.releasable.add(() -> Metrics.remove(name));
    }

    public void release() {
        this.releasable.forEach(ReleasableMetric::release);
    }

    @FunctionalInterface
    public interface ReleasableMetric
    {
        void release();
    }

    private static class SEPMetricNameFactory implements MetricNameFactory
    {
        @Override
        public CassandraMetricsRegistry.MetricName createMetricName(final String metricName)
        {
            final String groupName = SharedExecutorPoolMetrics.class.getPackage().getName();
            String mbeanName = groupName
                               + ':'
                               + "type=SharedExecutorPool"
                               + ",name=" + metricName;
            return new CassandraMetricsRegistry.MetricName(
                groupName,
                "SharedExecutorPool",
                "SharedExecutorPool " + metricName,
                mbeanName
            );
        }
    }
}
