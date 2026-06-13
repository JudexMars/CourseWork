package com.coursework.task1;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class CustomThreadPool implements CustomExecutor {

    private final PoolConfig config;
    private final NamedThreadFactory threadFactory;
    private final long keepAliveNanos;
    private final List<Worker> workers = new ArrayList<>();
    private final List<BlockingQueue<Runnable>> queues = new ArrayList<>();
    private final AtomicInteger roundRobinIndex = new AtomicInteger(0);
    private final AtomicBoolean shutdown = new AtomicBoolean(false);
    private final AtomicBoolean shutdownNow = new AtomicBoolean(false);
    private final Object workersLock = new Object();

    public CustomThreadPool(PoolConfig config) {
        this(config, new NamedThreadFactory("MyPool"));
    }

    public CustomThreadPool(PoolConfig config, NamedThreadFactory threadFactory) {
        this.config = config;
        this.threadFactory = threadFactory;
        this.keepAliveNanos = config.timeUnit().toNanos(config.keepAliveTime());

        synchronized (workersLock) {
            for (int i = 0; i < config.corePoolSize(); i++) {
                addWorkerLocked();
            }
        }
    }

    NamedThreadFactory getThreadFactory() {
        return threadFactory;
    }

    @Override
    public void execute(Runnable command) {
        if (command == null) {
            throw new NullPointerException("command");
        }
        if (shutdown.get()) {
            throw new RejectedExecutionException("Pool is shut down");
        }

        synchronized (workersLock) {
            ensureMinSpareThreadsLocked();

            int startIndex = Math.floorMod(roundRobinIndex.getAndIncrement(), Math.max(queues.size(), 1));
            for (int attempt = 0; attempt < queues.size(); attempt++) {
                int queueIndex = (startIndex + attempt) % queues.size();
                BlockingQueue<Runnable> queue = queues.get(queueIndex);
                if (queue.offer(command)) {
                    PoolLogger.pool("Task accepted into queue #" + queueIndex + ": " + describeTask(command));
                    return;
                }
            }

            if (workers.size() < config.maxPoolSize()) {
                int queueIndex = addWorkerLocked();
                BlockingQueue<Runnable> queue = queues.get(queueIndex);
                if (queue.offer(command)) {
                    PoolLogger.pool("Task accepted into queue #" + queueIndex + ": " + describeTask(command));
                    return;
                }
            }

            handleRejection(command);
        }
    }

    @Override
    public <T> Future<T> submit(Callable<T> callable) {
        if (callable == null) {
            throw new NullPointerException("callable");
        }
        FutureTask<T> futureTask = new FutureTask<>(callable);
        execute(futureTask);
        return futureTask;
    }

    @Override
    public void shutdown() {
        shutdown.set(true);
        PoolLogger.pool("Shutdown initiated. No new tasks will be accepted.");
        synchronized (workersLock) {
            for (Worker worker : workers) {
                worker.wakeUp();
            }
            workersLock.notifyAll();
        }
    }

    @Override
    public void shutdownNow() {
        shutdown.set(true);
        shutdownNow.set(true);
        PoolLogger.pool("ShutdownNow initiated. Interrupting workers.");

        synchronized (workersLock) {
            for (Worker worker : workers) {
                worker.stopWorker();
            }
            for (BlockingQueue<Runnable> queue : queues) {
                queue.clear();
            }
            workersLock.notifyAll();
        }
    }

    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        long deadline = System.nanoTime() + unit.toNanos(timeout);
        synchronized (workersLock) {
            while (!workers.isEmpty()) {
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) {
                    return workers.isEmpty();
                }
                workersLock.wait(TimeUnit.NANOSECONDS.toMillis(remaining));
            }
        }
        return true;
    }

    public int getActiveWorkerCount() {
        synchronized (workersLock) {
            return workers.size();
        }
    }

    public int getIdleWorkerCount() {
        synchronized (workersLock) {
            int idle = 0;
            for (Worker worker : workers) {
                if (!worker.isBusy()) {
                    idle++;
                }
            }
            return idle;
        }
    }

    boolean isShutdown() {
        return shutdown.get();
    }

    boolean isShutdownNow() {
        return shutdownNow.get();
    }

    boolean shouldTerminateIdleWorker() {
        synchronized (workersLock) {
            return workers.size() > config.corePoolSize();
        }
    }

    void onWorkerBecameIdle(Worker worker) {
        synchronized (workersLock) {
            ensureMinSpareThreadsLocked();
            workersLock.notifyAll();
        }
    }

    void onWorkerTerminated(Worker worker) {
        synchronized (workersLock) {
            int index = workers.indexOf(worker);
            if (index >= 0) {
                workers.remove(index);
                queues.remove(index);
            }
            workersLock.notifyAll();
        }
    }

    private void ensureMinSpareThreadsLocked() {
        while (getIdleWorkerCountLocked() < config.minSpareThreads()
                && workers.size() < config.maxPoolSize()) {
            addWorkerLocked();
        }
    }

    private int getIdleWorkerCountLocked() {
        int idle = 0;
        for (Worker worker : workers) {
            if (!worker.isBusy()) {
                idle++;
            }
        }
        return idle;
    }

    private int addWorkerLocked() {
        int workerId = workers.size();
        BlockingQueue<Runnable> queue = new ArrayBlockingQueue<>(config.queueSize());
        Worker worker = new Worker(workerId, queue, this, keepAliveNanos);
        queues.add(queue);
        workers.add(worker);
        worker.start();
        return workerId;
    }

    private void handleRejection(Runnable command) {
        String taskDescription = describeTask(command);
        switch (config.rejectionPolicy()) {
            case ABORT -> {
                PoolLogger.rejected("Task " + taskDescription + " was rejected due to overload!");
                throw new RejectedExecutionException("Task " + taskDescription + " rejected due to overload");
            }
            case CALLER_RUNS -> {
                PoolLogger.rejected("Task " + taskDescription + " will be executed in caller thread due to overload.");
                command.run();
            }
            case DISCARD -> PoolLogger.rejected("Task " + taskDescription + " was discarded due to overload!");
        }
    }

    private static String describeTask(Runnable task) {
        return task.getClass().getSimpleName() + "@" + Integer.toHexString(task.hashCode());
    }
}
