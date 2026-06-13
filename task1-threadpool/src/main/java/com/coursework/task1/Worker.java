package com.coursework.task1;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class Worker implements Runnable {

    private final int workerId;
    private final BlockingQueue<Runnable> queue;
    private final CustomThreadPool pool;
    private final long keepAliveNanos;
    private final Thread thread;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private volatile boolean busy;

    public Worker(int workerId, BlockingQueue<Runnable> queue, CustomThreadPool pool, long keepAliveNanos) {
        this.workerId = workerId;
        this.queue = queue;
        this.pool = pool;
        this.keepAliveNanos = keepAliveNanos;
        this.thread = pool.getThreadFactory().newThread(this);
    }

    public void start() {
        thread.start();
    }

    public Thread getThread() {
        return thread;
    }

    public int getWorkerId() {
        return workerId;
    }

    public boolean isBusy() {
        return busy;
    }

    public boolean isRunning() {
        return running.get() && thread.isAlive();
    }

    public void stopWorker() {
        running.set(false);
        thread.interrupt();
    }

    public void wakeUp() {
        thread.interrupt();
    }

    @Override
    public void run() {
        try {
            while (running.get()) {
                if (pool.isShutdown() && queue.isEmpty()) {
                    break;
                }

                Runnable task = queue.poll(keepAliveNanos, TimeUnit.NANOSECONDS);
                if (task == null) {
                    if (pool.shouldTerminateIdleWorker()) {
                        PoolLogger.worker(thread.getName() + " idle timeout, stopping.");
                        break;
                    }
                    continue;
                }

                if (pool.isShutdownNow()) {
                    queue.offer(task);
                    break;
                }

                busy = true;
                try {
                    PoolLogger.worker(thread.getName() + " executes " + describeTask(task));
                    task.run();
                } catch (RuntimeException ex) {
                    PoolLogger.worker(thread.getName() + " failed on " + describeTask(task) + ": " + ex.getMessage());
                    throw ex;
                } finally {
                    busy = false;
                    pool.onWorkerBecameIdle(this);
                }
            }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        } finally {
            running.set(false);
            pool.onWorkerTerminated(this);
            PoolLogger.worker(thread.getName() + " terminated.");
        }
    }

    private static String describeTask(Runnable task) {
        return task.getClass().getSimpleName() + "@" + Integer.toHexString(task.hashCode());
    }
}
