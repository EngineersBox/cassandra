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
    SEPWorkerMetrics metrics;

    // prevStopCheck stores the value of pool.stopCheck after we last incremented it; if it hasn't changed,
    // we know nobody else was spinning in the interval, so we increment our soleSpinnerSpinTime accordingly,
    // and otherwise we set it to zero; this is then used to terminate the final spinning thread, as the coordinated
    // strategy can only work when there are multiple threads spinning (as more sleep time must elapse than real time)
    long prevStopCheck = 0;
    long soleSpinnerSpinTime = 0;
    private long parkStart = 0;

    private final AtomicReference<Runnable> currentTask = new AtomicReference<>();

    SEPWorker(ThreadGroup threadGroup, Long workerId, Work initialState, SharedExecutorPool pool)
    {
        this.pool = pool;
        this.workerId = workerId;
        this.threadGroup = threadGroup;
        this.metrics = new SEPWorkerMetrics(
            threadGroup,
            workerId
        );
        this.metrics.setWorkStateOrdinal(initialState);
        thread = new FastThreadLocalThread(threadGroup, this, threadGroup.getName() + "-Worker-" + workerId);
        thread.setDaemon(true);
        set(initialState);
        thread.start();
    }

    /**
     * @return the current {@link DebuggableTask}, if one exists
     */
    public DebuggableTask currentDebuggableTask()
    {
        // can change after null check so go off local reference
        Runnable task = currentTask.get();

        // Local read and mutation Runnables are themselves debuggable
        if (task instanceof DebuggableTask)
            return (DebuggableTask) task;

        if (task instanceof FutureTask)
            return ((FutureTask<?>) task).debuggableTask();
            
        return null;
    }

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
        Runnable task = null;
        final long start = Clock.Global.nanoTime();
//        logger.info("[{}] Start run() {}", workerId, start);
        try
        {
            while (true)
            {
//                final long _start = Clock.Global.nanoTime();
//                logger.info("[{}] Start run() iteration {}", workerId, _start);
                if (pool.shuttingDown)
                {
                    this.metrics.runLatency.update(
                        Clock.Global.nanoTime() - start,
                        TimeUnit.NANOSECONDS
                    );
                    this.metrics.release();
                    return;
                }

                if (isSpinning() && !selfAssign())
                {
//                    logger.info("[{}] Is spinning + cannot self assign => doWaitSpin() {}", workerId, Clock.Global.nanoTime() - _start);
                    doWaitSpin();
//                    logger.info("[{}] End doWaitSpin() {}", workerId, Clock.Global.nanoTime() - _start);
                    // if the pool is terminating, but we have been assigned STOP_SIGNALLED, if we do not re-check
                    // whether the pool is shutting down this thread will go to sleep and block forever
                    continue;
                }

                // if stop was signalled, go to sleep (don't try self-assign; being put to sleep is rare, so let's obey it
                // whenever we receive it - though we don't apply this constraint to producers, who may reschedule us before
                // we go to sleep)
                if (stop())
                {
//                    logger.info(
//                        "[{}] Stop signalled, park worker until not-stopped {}",
//                        workerId,
//                        Clock.Global.nanoTime() - _start
//                    );
                    while (isStopped())
                    {
                        parkStart = Clock.Global.nanoTime();
                        LockSupport.park();
                    }
//                    logger.info("[{}] Finished top parking {}", workerId, Clock.Global.nanoTime() - _start);
                }

                // we can be assigned any state from STOPPED, so loop if we don't actually have any tasks assigned
                assigned = get().assigned;
                if (assigned == null)
                {
//                    logger.info("[{}] No tasks assigned {}", workerId, Clock.Global.nanoTime() - _start);
                    continue;
                }
                if (SET_THREAD_NAME)
                {
//                    logger.info(
//                        "[{}] Renamed thread {} => {}-{}",
//                        workerId,
//                        Thread.currentThread().getName(),
//                        assigned.name, workerId
//                    );
                    Thread.currentThread().setName(assigned.stageName + '-' + workerId);
                }
                this.metrics.setExecutorOrdinal(assigned.name);
//                logger.info(
//                    "[{}] Pending tasks before {} {}",
//                    workerId,
//                    assigned.tasks.size(),
//                    Clock.Global.nanoTime() - _start
//                );
                task = assigned.tasks.poll();
                currentTask.lazySet(task);

                // if we do have tasks assigned, nobody will change our state so we can simply set it to WORKING
                // (which is also a state that will never be interrupted externally)
                set(Work.WORKING);
//                logger.info("[{}] Set to WORKING state {}", workerId, Clock.Global.nanoTime() - _start);
                boolean shutdown;
                SEPExecutor.TakeTaskPermitResult status = null; // make sure set if shutdown check short circuits
                while (true)
                {

                    // before we process any task, we maybe schedule a new worker _to our executor only_; this
                    // ensures that even once all spinning threads have found work, if more work is left to be serviced
                    // and permits are available, it will be dealt with immediately.
//                    logger.info("[{}] maybeSchedule() {}", workerId, Clock.Global.nanoTime() - _start);
                    assigned.maybeSchedule();

                    // we know there is work waiting, as we have a work permit, so poll() will always succeed
                    final long startTask = Clock.Global.nanoTime();
//                    logger.info("[{}] Start task.run() {}", workerId, startTask - _start);
                    task.run();
                    this.metrics.taskRunLatency.update(
                        Clock.Global.nanoTime() - startTask,
                        TimeUnit.NANOSECONDS
                    );
//                    final long endTask = Clock.Global.nanoTime();
//                    logger.info("[{}] End task.run() {} Duration: {}", workerId, endTask - _start, endTask - startTask);
                    assigned.onCompletion();
                    task = null;

                    if (shutdown = assigned.shuttingDown)
                    {
//                        logger.info("[{}] Shutting down {}", workerId, Clock.Global.nanoTime() - _start);
                        break;
                    }

                    if (TOOK_PERMIT != (status = assigned.takeTaskPermit(true)))
                    {
//                        logger.info("[{}] Did not take permit {}", workerId, Clock.Global.nanoTime() - _start);
                        break;
                    }

//                    logger.info(
//                        "[{}] Pending tasks after WORKING iteration: {} {}",
//                        workerId,
//                        assigned.tasks.size(),
//                        Clock.Global.nanoTime() - _start
//                    );
                    task = assigned.tasks.poll();
                    currentTask.lazySet(task);
                }

                // return our work permit, and maybe signal shutdown
//                logger.info(
//                    "[{}] Release task {}",
//                    workerId,
//                    Clock.Global.nanoTime() - _start
//                );
                currentTask.lazySet(null);

                if (status != RETURNED_WORK_PERMIT)
                {
//                    logger.info("[{}] returnWorkPermit() {}", workerId, Clock.Global.nanoTime() - _start);
                    assigned.returnWorkPermit();
                }

                if (shutdown)
                {
//                    logger.info(
//                        "[{}] Shutdown invoked, active tasks: {} {}",
//                        workerId,
//                        assigned.getActiveTaskCount(),
//                        Clock.Global.nanoTime() - _start
//                    );
                    if (assigned.getActiveTaskCount() == 0)
                    {
//                        logger.info("[{}] Signalled shutdown all {}", workerId, Clock.Global.nanoTime() - _start);
                        assigned.shutdown.signalAll();
                    }
//                    final long end = Clock.Global.nanoTime();
//                    logger.info(
//                        "[{}] End run() {} Duration: {}",
//                        workerId,
//                        end,
//                        end - _start
//                    );
                    this.metrics.runLatency.update(
                        Clock.Global.nanoTime() - start,
                        TimeUnit.NANOSECONDS
                    );
                    return;
                }
                assigned = null;


                // try to immediately reassign ourselves some work; if we fail, start spinning
                if (!selfAssign())
                {
//                    logger.info(
//                        "[{}] Cannot self assign, start SPINNING {}",
//                        workerId,
//                        Clock.Global.nanoTime() - _start
//                    );
                    startSpinning();
                }
//                } else {
//                    logger.info("[{}] Self assigned {}", workerId, Clock.Global.nanoTime() - _start);
//                }
//                final long end = Clock.Global.nanoTime();
//                logger.info(
//                    "[{}] End run() iteration {} Duration: {}",
//                    workerId,
//                    end,
//                    end - _start
//                );
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
                    set(Work.WORKING);
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
//            logger.info("[{}] Finally released task {}", workerId, Clock.Global.nanoTime() - start);
            currentTask.lazySet(null);
//            logger.info("[{}] Marked workerEnded {}", workerId, Clock.Global.nanoTime() - start);
            pool.workerEnded(this);
        }
//        final long end = Clock.Global.nanoTime();
//        logger.info("[{}] End run() {} Duration: {}", workerId, end, end - start);
    }

    // try to assign this worker the provided work
    // valid states to assign are SPINNING, STOP_SIGNALLED, (ASSIGNED);
    // restores invariants of the various states (e.g. spinningCount, descheduled collection and thread park status)
    boolean assign(Work work, boolean self)
    {
        Work state = get();
        final long start = Clock.Global.nanoTime();
//        logger.info(
//            "[{}] Start assign({}, {}) {}",
//            workerId,
//            work.label,
//            self,
//            start
//        );
        // Note that this loop only performs multiple iterations when
        // CAS on aquiring work fails. Every other case terminates the
        // loop and the method call.
        while (state.canAssign(self))
        {
            if (!compareAndSet(state, work))
            {
//                logger.info("[{}] CAS work state failed {}", workerId, Clock.Global.nanoTime() - start);
                state = get();
                continue;
            }
//            logger.info("[{}] CAS work state succeeded {}", workerId, Clock.Global.nanoTime() - start);
            // if we were spinning, exit the state (decrement the count); this is valid even if we are already spinning,
            // as the assigning thread will have incremented the spinningCount
            if (state.isSpinning())
                stopSpinning();

            // if we're being descheduled, place ourselves in the descheduled collection
            if (work.isStop())
            {
//                logger.info("[{}] Stopped, descheduling {}", workerId, Clock.Global.nanoTime() - start);
                pool.descheduled.put(workerId, this);
                if (pool.shuttingDown)
                {
//                    final long end = Clock.Global.nanoTime();
//                    logger.info(
//                        "[{}] Pool shudown, end assign({},{}) {} Duration: {}",
//                        workerId,
//                        work.label,
//                        self,
//                        end,
//                        end - start
//                    );
                    this.metrics.setWorkStateOrdinal(work);
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
//                logger.info(
//                    "[{}] Stopped and next state is not stopped, unparking {}",
//                    workerId,
//                    Clock.Global.nanoTime() - start
//                );
                LockSupport.unpark(thread);
                this.metrics.parkLatency.update(
                    Clock.Global.nanoTime() - parkStart,
                    TimeUnit.NANOSECONDS
                );
            }
//            final long end = Clock.Global.nanoTime();
//            logger.info(
//                "[{}] End assign({},{}) success {} Duration: {}",
//                workerId,
//                work.label,
//                self,
//                end,
//                end - start
//            );
            this.metrics.setWorkStateOrdinal(work);
            this.metrics.assignLatency.update(
                Clock.Global.nanoTime() - start,
                TimeUnit.NANOSECONDS
            );
            return true;
        }
//        final long end = Clock.Global.nanoTime();
//        logger.info(
//            "[{}] Assign end failure {} Duration: {}",
//            workerId,
//            end,
//            end - start
//        );
        this.metrics.setWorkStateOrdinal(work);
        this.metrics.assignLatency.update(
            Clock.Global.nanoTime() - start,
            TimeUnit.NANOSECONDS
        );
        return false;
    }

    // try to assign ourselves an executor with work available
    private boolean selfAssign()
    {
        final long start = Clock.Global.nanoTime();
//        logger.info("[{}] Start selfAssign() {}", workerId, start);
        // if we aren't permitted to assign in this state, fail
        if (!get().canAssign(true))
        {
//            final long end = Clock.Global.nanoTime();
//            logger.info(
//                "[{}] End selfAssign() cannot assign {} Duration: {}",
//                workerId,
//                end,
//                end - start
//            );
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
//                    final long end = Clock.Global.nanoTime();
//                    logger.info(
//                        "[{}] Self assign succeeded on current worker {} Duration: {}",
//                        workerId,
//                        end,
//                        end - start
//                    );
                    this.metrics.selfAssignLatency.update(
                        Clock.Global.nanoTime() - start,
                        TimeUnit.NANOSECONDS
                    );
                    return true;
                }
                // ... if we fail, schedule it to another worker
//                logger.info("[{}] Scheduling work to another worker {}", workerId, Clock.Global.nanoTime() - start);
                pool.schedule(work);
//                final long end = Clock.Global.nanoTime();
//                logger.info(
//                    "[{}] End selfAssign() successful {} Duration: {}",
//                    workerId,
//                    end,
//                    end - start
//                );
                // and return success as we must have already been assigned a task
                assert get().assigned != null;
                this.metrics.selfAssignLatency.update(
                    Clock.Global.nanoTime() - start,
                    TimeUnit.NANOSECONDS
                );
                return true;
            }
        }
//        final long end = Clock.Global.nanoTime();
//        logger.info(
//            "[{}] End selfAssign() failure {} Duration: {}",
//            workerId,
//            end,
//            end - start
//        );
        this.metrics.selfAssignLatency.update(
            Clock.Global.nanoTime() - start,
            TimeUnit.NANOSECONDS
        );
        return false;
    }

    // we can only call this when our state is WORKING, and no other thread may change our state in this case;
    // so in this case only we do not need to CAS. We increment the spinningCount and add ourselves to the spinning
    // collection at the same time
    private void startSpinning()
    {
//        logger.info("[{}] startSpinning() {}", workerId, Clock.Global.nanoTime());
        assert get() == Work.WORKING;
        pool.spinningCount.incrementAndGet();
        set(Work.SPINNING);
    }

    // exit the spinning state; if there are no remaining spinners, we immediately try and schedule work for all executors
    // so that any producer is safe to not spin up a worker when they see a spinning thread (invariant (1) above)
    private void stopSpinning()
    {
//        final long start = Clock.Global.nanoTime();
//        logger.info("[{}] Start stopSpinning() {}", workerId, start);
        if (pool.spinningCount.decrementAndGet() == 0)
        {
//            logger.info(
//                "[{}] No more spinning, scheduling executor. Num: {} {}",
//                workerId,
//                pool.executors.size(),
//                Clock.Global.nanoTime() - start
//            );
            for (SEPExecutor executor : pool.executors)
            {
                final boolean result = executor.maybeSchedule();
//                logger.info(
//                    "[{}] maybeSchedule() executor {} => {} {}",
//                    workerId,
//                    executor.name,
//                    result,
//                    Clock.Global.nanoTime() - start
//                );
            }
        }
        prevStopCheck = soleSpinnerSpinTime = 0;
//        final long end = Clock.Global.nanoTime();
//        logger.info(
//            "[{}] End stopSpinning() {} Duration: {}",
//            workerId,
//            end,
//            end - start
//        );
    }

    // perform a sleep-spin, incrementing pool.stopCheck accordingly
    private void doWaitSpin()
    {
        final long _start = Clock.Global.nanoTime();
//        logger.info("[{}] Start doWaitSpin() {}", workerId, start);
        // pick a random sleep interval based on the number of threads spinning, so that
        // we should always have a thread about to wake up, but most threads are sleeping
        long sleep = 10000L * pool.spinningCount.get();
        sleep = Math.min(1000000, sleep);
        sleep *= ThreadLocalRandom.current().nextDouble();
        sleep = Math.max(10000, sleep);

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
//        logger.info("[{}] End worker doWaitSpin() {} Duration: {}", workerId, end, spin);
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
    private void maybeStop(long stopCheck, long now)
    {
        final long start = Clock.Global.nanoTime();
//        logger.info("[{}] Start maybeStop({},{}) {}", workerId, stopCheck, now, start);
        long delta = now - stopCheck;
        if (delta <= 0)
        {
//            logger.info("[{}] Negative delta {}", workerId, Clock.Global.nanoTime() - start);
            // if stopCheck has caught up with present, we've been spinning too much, so if we can atomically
            // set it to the past again, we should stop a worker
            if (pool.stopCheck.compareAndSet(stopCheck, now - stopCheckInterval))
            {
//                logger.info(
//                    "[{}] CAS succeeded for stopCheck to {} {}",
//                    workerId,
//                    now - stopCheckInterval,
//                    Clock.Global.nanoTime() - start
//                );
                // try and stop ourselves;
                // if we've already been assigned work stop another worker
                if (!assign(Work.STOP_SIGNALLED, true))
                {
//                    logger.info(
//                        "[{}] Cannot assign STOP_SIGNALLED, scheduling {}",
//                        workerId,
//                        Clock.Global.nanoTime() - start
//                    );
                    pool.schedule(Work.STOP_SIGNALLED);
                }
//                } else {
//                    logger.info("[{}] Assigned STOP_SIGNALLED {}", workerId, Clock.Global.nanoTime() - start);
//                }
            }
//            } else {
//                logger.info(
//                    "[{}] CAS failed for stopCheck to {} {}",
//                    workerId,
//                    now - stopCheckInterval,
//                    Clock.Global.nanoTime() - start
//                );
//            }
        }
        else if (soleSpinnerSpinTime > stopCheckInterval && pool.spinningCount.get() == 1)
        {
//            logger.info(
//                "[{}] Spin time greater than interval and spun only once, assigning STOP_SIGNALLED {}",
//                workerId,
//                Clock.Global.nanoTime() - start
//            );
            // permit self-stopping
            assign(Work.STOP_SIGNALLED, true);
        }
        else
        {
//            logger.info(
//                "[{}] Start resetting stopCheck as too far in past {}",
//                workerId,
//                Clock.Global.nanoTime() - start
//            );
            // if stop check has gotten too far behind present, update it so new spins can affect it
            while (delta > stopCheckInterval * 2 && !pool.stopCheck.compareAndSet(stopCheck, now - stopCheckInterval))
            {
                stopCheck = pool.stopCheck.get();
                delta = now - stopCheck;
            }
//            logger.info(
//                "[{}] Finished resetting stopCheck {}",
//                workerId,
//                Clock.Global.nanoTime() - start
//            );
        }
//        final long end = Clock.Global.nanoTime();
//        logger.info(
//            "[{}] End maybeStop({},{}) {} Duration: {}",
//            workerId,
//            stopCheck,
//            now,
//            end,
//            end - start
//        );
        this.metrics.stopLatency.update(
            Clock.Global.nanoTime() - start,
            TimeUnit.NANOSECONDS
        );
    }

    private boolean isSpinning()
    {
        return get().isSpinning();
    }

    private boolean stop()
    {
        return get().isStop() && compareAndSet(Work.STOP_SIGNALLED, Work.STOPPED);
    }

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
