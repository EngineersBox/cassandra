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

public class SEPWorkerMetrics
{
    private final SEPWorkerMetricNameFactory nameFactory;
    private final Set<ReleasableMetric> releasable = new HashSet<>();

    public final Timer runLatency;
    public final Timer taskRunLatency;
    public final Timer parkLatency;
    public final Timer waitSpinLatency;
    public final Timer assignLatency;
    public final Timer selfAssignLatency;
    public final Timer stopLatency;

    public SEPWorkerMetrics(final ThreadGroup threadGroup, final Thread worker) {
        this.nameFactory = new SEPWorkerMetricNameFactory(
            threadGroup.getName(),
            worker
        );
        this.runLatency = timer("RunLatency");
        this.taskRunLatency = timer("TaskRunLatency");
        this.parkLatency = timer("ParkLatency");
        this.waitSpinLatency = timer("WaitSpinLatency");
        this.assignLatency = timer("AssignLatency");
        this.selfAssignLatency = timer("SelfAssignLatency");
        this.stopLatency = timer("StopLatency");
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

    private static class SEPWorkerMetricNameFactory implements MetricNameFactory
    {

        private final String threadGroup;
        private final Thread worker;

        public SEPWorkerMetricNameFactory(final String threadGroup,
                                          final Thread worker) {
            this.threadGroup = threadGroup;
            this.worker = worker;
        }

        @Override
        public CassandraMetricsRegistry.MetricName createMetricName(final String metricName)
        {
            final String groupName = SEPWorkerMetrics.class.getPackage().getName();
            final String workerName = this.worker.getName();
            final String mbeanName = groupName
                               + ':'
                               + "type=SEPWorker,"
                               + "threadgroup=" + this.threadGroup
                               + ",scope=" + workerName
                               + ",name=" + metricName;
            return new CassandraMetricsRegistry.MetricName(
                groupName,
                "SEPWorker",
                metricName,
                workerName,
                mbeanName
            );
        }
    }

}
