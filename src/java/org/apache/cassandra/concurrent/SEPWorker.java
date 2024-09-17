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
package org.apache.cassandra.concurrent;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.util.concurrent.FastThreadLocalThread;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.instrumentation.annotations.SpanAttribute;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import org.apache.cassandra.metrics.scheduler.SEPWorkerMetrics;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.JVMStabilityInspector;

import static org.apache.cassandra.concurrent.SEPExecutor.TakeTaskPermitResult.RETURNED_WORK_PERMIT;
import static org.apache.cassandra.concurrent.SEPExecutor.TakeTaskPermitResult.TOOK_PERMIT;
import static org.apache.cassandra.config.CassandraRelevantProperties.SET_SEP_THREAD_NAME;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;

public final class SEPWorker extends AtomicReference<SEPWorker.Work> implements Runnable
{
    private static final Logger logger = LoggerFactory.getLogger(SEPWorker.class);
    private static final boolean SET_THREAD_NAME = SET_SEP_THREAD_NAME.getBoolean();

    final Long workerId;
    final Thread thread;
    final SharedExecutorPool pool;
    final ThreadGroup threadGroup;
    Span parkSpan;
    SEPWorkerMetrics metrics;
    private final Tracer tracer;

    // prevStopCheck stores the value of pool.stopCheck after we last incremented it; if it hasn't changed,
    // we know nobody else was spinning in the interval, so we increment our soleSpinnerSpinTime accordingly,
    // and otherwise we set it to zero; this is then used to terminate the final spinning thread, as the coordinated
    // strategy can only work when there are multiple threads spinning (as more sleep time must elapse than real time)
    long prevStopCheck = 0;
    long soleSpinnerSpinTime = 0;
    private long parkStart = 0;

    private final AtomicReference<ContextualTask> currentTask = new AtomicReference<>();

    SEPWorker(ThreadGroup threadGroup, Long workerId, Work initialState, SharedExecutorPool pool)
    {
        this.pool = pool;
        this.workerId = workerId;
        this.threadGroup = threadGroup;
        this.metrics = new SEPWorkerMetrics(
            threadGroup,
            workerId
        );
        this.tracer = GlobalOpenTelemetry.getTracerProvider().get("SEPWorker");
        thread = new FastThreadLocalThread(threadGroup, this, threadGroup.getName() + "-Worker-" + workerId);
        thread.setDaemon(true);
        setWork(initialState);
        thread.start();
    }

    @WithSpan
    public void setWork(@SpanAttribute("work") final SEPWorker.Work work) {
        set(work);
        this.metrics.setWorkStateOrdinal(work);
    }

    /**
     * @return the current {@link DebuggableTask}, if one exists
     */
    public DebuggableTask currentDebuggableTask()
    {
        // can change after null check so go off local reference
        final ContextualTask task = currentTask.get();
        if (task == null) {
            return null;
        }

        // Local read and mutation Runnables are themselves debuggable
        if (task.runnable instanceof DebuggableTask)
            return (DebuggableTask) task;

        if (task.runnable instanceof FutureTask)
            return ((FutureTask<?>) task.runnable).debuggableTask();
            
        return null;
    }


    @WithSpan
    public void run()
    {
        /*
         * we maintain two important invariants:
         * 1)   after exiting spinning phase, we ensure at least one more task on _each_ queue will be processed
         *      promptly after we begin, assuming any are outstanding on any pools. this is to permit producers to
         *      avoid signalling if there are _any_ spinning threads. we achieve this by simply calling maybeSchedule()
         *      on each queue if on decrementing the spin counter we hit zero.
         * 2)   before processing a task on a given queue, we attempt to assign another worker to the _same queue only_;
         *      this allows a producer to skip signalling work if the task queue is currently non-empty, and in conjunction
         *      with invariant (1) ensures that if any thread was spinning when a task was added to any executor, that
         *      task will be processed immediately if work permits are available
         */

        SEPExecutor assigned = null;
        ContextualTask task = null;
        final long start = Clock.Global.nanoTime();
        Span span;
        try
        {
            while (true)
            {
                span = this.tracer.spanBuilder("SEPWorker::runLoop")
                                  .setParent(Context.current())
                                  .startSpan();
                if (pool.shuttingDown)
                {
                    span.addEvent("Shutting down");
                    span.end();
                    this.metrics.runLatency.update(
                        Clock.Global.nanoTime() - start,
                        TimeUnit.NANOSECONDS
                    );
                    this.metrics.release();
                    return;
                }

                if (isSpinning() && !selfAssign())
                {
                    doWaitSpin();
                    // if the pool is terminating, but we have been assigned STOP_SIGNALLED, if we do not re-check
                    // whether the pool is shutting down this thread will go to sleep and block forever
                    span.end();
                    continue;
                }

                // if stop was signalled, go to sleep (don't try self-assign; being put to sleep is rare, so let's obey it
                // whenever we receive it - though we don't apply this constraint to producers, who may reschedule us before
                // we go to sleep)
                if (stop())
                {
                    while (isStopped())
                    {
                        this.parkStart = Clock.Global.nanoTime();
                        this.parkSpan = this.tracer.spanBuilder("LockSupport.park")
                                              .setParent(Context.current().with(span))
                                              .startSpan();
                        LockSupport.park();
                    }
                }

                // we can be assigned any state from STOPPED, so loop if we don't actually have any tasks assigned
                assigned = get().assigned;
                if (assigned == null)
                {
                    span.addEvent("No SEPExecutor assigned");
                    span.end();
                    continue;
                }
                if (SET_THREAD_NAME)
                {
                    Thread.currentThread().setName(assigned.stageName + '-' + workerId);
                }
                this.metrics.setExecutorOrdinal(assigned.name);
                task = assigned.tasks.poll();
                currentTask.lazySet(task);

                // if we do have tasks assigned, nobody will change our state so we can simply set it to WORKING
                // (which is also a state that will never be interrupted externally)
                setWork(Work.WORKING);
                boolean shutdown;
                SEPExecutor.TakeTaskPermitResult status = null; // make sure set if shutdown check short circuits
                final Span innerSpan = this.tracer.spanBuilder("SEPWorker::innerTaskLoop")
                                            .setParent(Context.current().with(span))
                                            .startSpan();
                while (true)
                {

                    // before we process any task, we maybe schedule a new worker _to our executor only_; this
                    // ensures that even once all spinning threads have found work, if more work is left to be serviced
                    // and permits are available, it will be dealt with immediately.
//                    logger.info("[{}] maybeSchedule() {}", workerId, Clock.Global.nanoTime() - _start);
                    innerSpan.addEvent("Attempting to schedule threads to assigned SEPExecutor");
                    assigned.maybeSchedule();

                    // we know there is work waiting, as we have a work permit, so poll() will always succeed
                    final Span taskSpan = this.tracer.spanBuilder("Runnable.run()")
                                                     .setParent(Context.current().with(innerSpan))
                                                     .startSpan();
                    final Span ctxSpan = this.tracer.spanBuilder("ContextualTask.runnable.run()")
                                                    .setParent(task.context)
                                                    .addLink(taskSpan.getSpanContext())
                                                    .startSpan();
                    final long startTask = Clock.Global.nanoTime();
                    task.runnable.run();
                    this.metrics.taskRunLatency.update(
                        Clock.Global.nanoTime() - startTask,
                        TimeUnit.NANOSECONDS
                    );
                    ctxSpan.end();
                    taskSpan.end();
                    assigned.onCompletion();
                    task = null;

                    if (shutdown = assigned.shuttingDown)
                    {
                        innerSpan.addEvent("Assigned SEPExecutor Shutting Down");
                        break;
                    }

                    if (TOOK_PERMIT != (status = assigned.takeTaskPermit(true)))
                    {
                        innerSpan.addEvent("Could not take task permit");
                        break;
                    }

                    task = assigned.tasks.poll();
                    currentTask.lazySet(task);
                }
                innerSpan.end();
                // return our work permit, and maybe signal shutdown
                currentTask.lazySet(null);

                if (status != RETURNED_WORK_PERMIT)
                {
                    assigned.returnWorkPermit();
                }

                if (shutdown)
                {
                    if (assigned.getActiveTaskCount() == 0)
                    {
                        assigned.shutdown.signalAll();
                    }
                    this.metrics.runLatency.update(
                        Clock.Global.nanoTime() - start,
                        TimeUnit.NANOSECONDS
                    );
                    span.end();
                    return;
                }
                assigned = null;


                // try to immediately reassign ourselves some work; if we fail, start spinning
                if (!selfAssign())
                {
                    startSpinning();
                }
                span.end();
            }
        }
        catch (Throwable t)
        {
            JVMStabilityInspector.inspectThrowable(t);
            while (true)
            {
                if (get().assigned != null)
                {
                    assigned = get().assigned;
                    setWork(Work.WORKING);
                }
                if (assign(Work.STOPPED, true))
                    break;
            }
            if (assigned != null)
                assigned.returnWorkPermit();
            if (task != null)
            {
                logger.error("Failed to execute task, unexpected exception killed worker", t);
                assigned.onCompletion();
            }
            else
            {
                logger.error("Unexpected exception killed worker", t);
            }
        }
        finally
        {
            this.metrics.runLatency.update(
                Clock.Global.nanoTime() - start,
                TimeUnit.NANOSECONDS
            );
            currentTask.lazySet(null);
            pool.workerEnded(this);
        }
    }

    // try to assign this worker the provided work
    // valid states to assign are SPINNING, STOP_SIGNALLED, (ASSIGNED);
    // restores invariants of the various states (e.g. spinningCount, descheduled collection and thread park status)
    @WithSpan
    boolean assign(@SpanAttribute("work") Work work,
                   @SpanAttribute("self") boolean self)
    {
        Work state = get();
        final long start = Clock.Global.nanoTime();
        Span span;
        // Note that this loop only performs multiple iterations when
        // CAS on aquiring work fails. Every other case terminates the
        // loop and the method call.
        while (state.canAssign(self))
        {
            span = this.tracer.spanBuilder("SEPWorker.state.compareAndSet")
                   .setParent(Context.current())
                   .startSpan();
            if (!compareAndSet(state, work))
            {
                state = get();
                span.setStatus(StatusCode.ERROR);
                span.end();
                continue;
            }
            span.setStatus(StatusCode.OK);
            span.end();
            this.metrics.setWorkStateOrdinal(work);
            // if we were spinning, exit the state (decrement the count); this is valid even if we are already spinning,
            // as the assigning thread will have incremented the spinningCount
            if (state.isSpinning())
                stopSpinning();

            // if we're being descheduled, place ourselves in the descheduled collection
            if (work.isStop())
            {
                pool.descheduled.put(workerId, this);
                if (pool.shuttingDown)
                {
                    this.metrics.assignLatency.update(
                        Clock.Global.nanoTime() - start,
                        TimeUnit.NANOSECONDS
                    );
                    return true;
                }
            }

            // if we're currently stopped, and the new state is not a stop signal
            // (which we can immediately convert to stopped), unpark the worker
            if (state.isStopped() && (!work.isStop() || !stop()))
            {
                LockSupport.unpark(thread);
                final long parkEnd = Clock.Global.nanoTime();
                if (this.parkSpan != null)
                {
                    this.parkSpan.end();
                }
                this.metrics.parkLatency.update(
                    parkEnd - parkStart,
                    TimeUnit.NANOSECONDS
                );
            }
            this.metrics.assignLatency.update(
                Clock.Global.nanoTime() - start,
                TimeUnit.NANOSECONDS
            );
            return true;
        }
        this.metrics.assignLatency.update(
            Clock.Global.nanoTime() - start,
            TimeUnit.NANOSECONDS
        );
        return false;
    }

    // try to assign ourselves an executor with work available
    @WithSpan
    private boolean selfAssign()
    {
        final long start = Clock.Global.nanoTime();
        // if we aren't permitted to assign in this state, fail
        if (!get().canAssign(true))
        {
            this.metrics.selfAssignLatency.update(
                Clock.Global.nanoTime() - start,
                TimeUnit.NANOSECONDS
            );
            return false;
        }
        for (SEPExecutor exec : pool.executors)
        {
            if (exec.takeWorkPermit(true))
            {
                Work work = new Work(exec);
                // we successfully started work on this executor, so we must either assign it to ourselves or ...
                if (assign(work, true))
                {
                    this.metrics.selfAssignLatency.update(
                        Clock.Global.nanoTime() - start,
                        TimeUnit.NANOSECONDS
                    );
                    return true;
                }
                // ... if we fail, schedule it to another worker
                pool.schedule(work);
                // and return success as we must have already been assigned a task
                assert get().assigned != null;
                this.metrics.selfAssignLatency.update(
                    Clock.Global.nanoTime() - start,
                    TimeUnit.NANOSECONDS
                );
                return true;
            }
        }
        this.metrics.selfAssignLatency.update(
            Clock.Global.nanoTime() - start,
            TimeUnit.NANOSECONDS
        );
        return false;
    }

    // we can only call this when our state is WORKING, and no other thread may change our state in this case;
    // so in this case only we do not need to CAS. We increment the spinningCount and add ourselves to the spinning
    // collection at the same time
    @WithSpan
    private void startSpinning()
    {
        assert get() == Work.WORKING;
        pool.spinningCount.incrementAndGet();
        setWork(Work.SPINNING);
    }

    // exit the spinning state; if there are no remaining spinners, we immediately try and schedule work for all executors
    // so that any producer is safe to not spin up a worker when they see a spinning thread (invariant (1) above)
    @WithSpan
    private void stopSpinning()
    {
        if (pool.spinningCount.decrementAndGet() == 0)
        {
            for (SEPExecutor executor : pool.executors)
            {
                executor.maybeSchedule();
            }
        }
        prevStopCheck = soleSpinnerSpinTime = 0;
    }

    // perform a sleep-spin, incrementing pool.stopCheck accordingly
    @WithSpan
    private void doWaitSpin()
    {
        final long _start = Clock.Global.nanoTime();
        // pick a random sleep interval based on the number of threads spinning, so that
        // we should always have a thread about to wake up, but most threads are sleeping
        long sleep = 10000L * pool.spinningCount.get();
        sleep = Math.min(1000000, sleep);
        sleep *= ThreadLocalRandom.current().nextDouble();
        sleep = Math.max(10000, sleep);

        // Minimise sleep operations to see if throughput improves

        long start = nanoTime();

        // place ourselves in the spinning collection; if we clash with another thread just exit
        Long target = start + sleep;
        if (pool.spinning.putIfAbsent(target, this) != null)
            LockSupport.parkNanos(sleep);

        // remove ourselves (if haven't been already) - we should be at or near the front, so should be cheap-ish
        pool.spinning.remove(target, this);

        // finish timing and grab spinningTime (before we finish timing so it is under rather than overestimated)
        long end = nanoTime();
        long spin = end - start;
        long stopCheck = pool.stopCheck.addAndGet(spin);
        maybeStop(stopCheck, end);
        if (prevStopCheck + spin == stopCheck)
            soleSpinnerSpinTime += spin;
        else
            soleSpinnerSpinTime = 0;
        prevStopCheck = stopCheck;
        this.metrics.waitSpinLatency.update(
            Clock.Global.nanoTime() - _start,
            TimeUnit.NANOSECONDS
        );
    }

    private static final long stopCheckInterval = TimeUnit.MILLISECONDS.toNanos(10L);

    // stops a worker if elapsed real time is less than elapsed spin time, as this implies the equivalent of
    // at least one worker achieved nothing in the interval. we achieve this by maintaining a stopCheck which
    // is initialised to a negative offset from realtime; as we spin we add to this value, and if we ever exceed
    // realtime we have spun too much and deschedule; if we get too far behind realtime, we reset to our initial offset
    @WithSpan
    private void maybeStop(@SpanAttribute("stopCheck") long stopCheck,
                           @SpanAttribute("now") long now)
    {
        final long start = Clock.Global.nanoTime();
        long delta = now - stopCheck;
        if (delta <= 0)
        {
            // if stopCheck has caught up with present, we've been spinning too much, so if we can atomically
            // set it to the past again, we should stop a worker
            if (pool.stopCheck.compareAndSet(stopCheck, now - stopCheckInterval))
            {
                // try and stop ourselves;
                // if we've already been assigned work stop another worker
                if (!assign(Work.STOP_SIGNALLED, true))
                {
                    pool.schedule(Work.STOP_SIGNALLED);
                }
            }
        }
        else if (soleSpinnerSpinTime > stopCheckInterval && pool.spinningCount.get() == 1)
        {
            // permit self-stopping
            assign(Work.STOP_SIGNALLED, true);
        }
        else
        {
            // if stop check has gotten too far behind present, update it so new spins can affect it
            while (delta > stopCheckInterval * 2 && !pool.stopCheck.compareAndSet(stopCheck, now - stopCheckInterval))
            {
                stopCheck = pool.stopCheck.get();
                delta = now - stopCheck;
            }
        }
        this.metrics.stopLatency.update(
            Clock.Global.nanoTime() - start,
            TimeUnit.NANOSECONDS
        );
    }

    @WithSpan
    private boolean isSpinning()
    {
        return get().isSpinning();
    }

    @WithSpan
    private boolean stop()
    {
        return get().isStop() && compareAndSet(Work.STOP_SIGNALLED, Work.STOPPED);
    }

    @WithSpan
    private boolean isStopped()
    {
        return get().isStopped();
    }

    /**
     * Represents, and communicates changes to, a worker's work state - there are three non-actively-working
     * states (STOP_SIGNALLED, STOPPED, AND SPINNING) and two working states: WORKING, and (ASSIGNED), the last
     * being represented by a non-static instance with its "assigned" executor set.
     *
     * STOPPED:         indicates the worker is descheduled, and whilst accepts work in this state (causing it to
     *                  be rescheduled) it will generally not be considered for work until all other worker threads are busy.
     *                  In this state we should be present in the pool.descheduled collection, and should be parked
     * -> (ASSIGNED)|SPINNING
     * STOP_SIGNALLED:  the worker has been asked to deschedule itself, but has not yet done so; only entered from a SPINNING
     *                  state, and generally communicated to itself, but maybe set from any worker. this state may be preempted
     *                  and replaced with (ASSIGNED) or SPINNING
     *                  In this state we should be present in the pool.descheduled collection
     * -> (ASSIGNED)|STOPPED|SPINNING
     * SPINNING:        indicates the worker has no work to perform, so is performing a friendly wait-based-spinning
     *                  until it either is (ASSIGNED) some work (by itself or another thread), or sent STOP_SIGNALLED
     *                  In this state we _may_ be in the pool.spinning collection (but only if we are in the middle of a sleep)
     * -> (ASSIGNED)|STOP_SIGNALLED|SPINNING
     * (ASSIGNED):      asks the worker to perform some work against the specified executor, and preassigns a task permit
     *                  from that executor so that in this state there is always work to perform.
     *                  In general a worker assigns itself this state, but sometimes it may assign another worker the state
     *                  either if there is work outstanding and no-spinning threads, or there is a race to self-assign
     * -> WORKING
     * WORKING:         indicates the worker is actively processing an executor's task queue; in this state it accepts
     *                  no state changes/communications, except from itself; it usually exits this mode into SPINNING,
     *                  but if work is immediately available on another executor it self-triggers (ASSIGNED)
     * -> SPINNING|(ASSIGNED)
     */

    public static final class Work
    {
        public static final Work STOP_SIGNALLED = new Work("STOPPED_SIGNALLED");
        public static final Work STOPPED = new Work("STOPPED");
        public static final Work SPINNING = new Work("SPINNING");
        public static final Work WORKING = new Work("WORKING");

        final SEPExecutor assigned;
        public final String label;

        Work(SEPExecutor executor)
        {
            assert(executor != null);
            this.assigned = executor;
            this.label = "ASSIGNED";
        }

        private Work(final String label)
        {
            this.assigned = null;
            this.label = label;
        }

        boolean canAssign(boolean self)
        {
            // we can assign work if there isn't new work already assigned and either
            // 1) we are assigning to ourselves
            // 2) the worker we are assigning to is not already in the middle of WORKING
            return assigned == null && (self || !isWorking());
        }

        boolean isSpinning()
        {
            return this == Work.SPINNING;
        }

        boolean isWorking()
        {
            return this == Work.WORKING;
        }

        boolean isStop()
        {
            return this == Work.STOP_SIGNALLED;
        }

        boolean isStopped()
        {
            return this == Work.STOPPED;
        }

        boolean isAssigned()
        {
            return assigned != null;
        }

        boolean isRunnableState() {
            return isAssigned() || isWorking();
        }

        @Override
        public String toString() {
            return this.label;
        }

    }

    @Override
    public String toString()
    {
        return thread.getName();
    }

    @Override
    public int hashCode()
    {
        return workerId.intValue();
    }

    @Override
    public boolean equals(Object obj)
    {
        return obj == this;
    }
}
