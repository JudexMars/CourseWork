package com.coursework.task1;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class Main {

    private static final Logger LOGGER = Logger.getLogger(Main.class.getName());

    public static void main(String[] args) throws Exception {
        if (args.length > 0 && "benchmark".equals(args[0])) {
            runBenchmarkComparison();
            System.out.println();
            runParameterResearch();
            return;
        }

        demonstrateNormalWork();
        System.out.println();
        demonstrateOverload();
        System.out.println();
        demonstrateShutdown();
        System.out.println();
        runBenchmarkComparison();
        System.out.println();
        runParameterResearch();
    }

    private static void demonstrateNormalWork() throws Exception {
        LOGGER.info("=== Demo 1: Normal work ===");

        PoolConfig config = new PoolConfig(
                2, 4, 5, TimeUnit.SECONDS, 5, 1, RejectionPolicy.ABORT
        );
        CustomThreadPool pool = new CustomThreadPool(config);

        List<Future<String>> futures = new ArrayList<>();
        for (int taskId = 1; taskId <= 12; taskId++) {
            futures.add(pool.submit(sleepingTask(taskId, 500 + (taskId % 3) * 500L)));
        }

        for (Future<String> future : futures) {
            LOGGER.info("Result: " + future.get(30, TimeUnit.SECONDS));
        }

        pool.shutdown();
        boolean terminated = pool.awaitTermination(30, TimeUnit.SECONDS);
        LOGGER.info("Pool terminated after normal work: " + terminated
                + ", active workers: " + pool.getActiveWorkerCount());
    }

    private static void demonstrateOverload() {
        LOGGER.info("=== Demo 2: Overload / rejection ===");

        PoolConfig config = new PoolConfig(
                2, 2, 2, TimeUnit.SECONDS, 2, 0, RejectionPolicy.ABORT
        );
        CustomThreadPool pool = new CustomThreadPool(config);

        int accepted = 0;
        int rejected = 0;

        for (int taskId = 1; taskId <= 25; taskId++) {
            try {
                pool.execute(sleepingRunnable(taskId, 200));
                accepted++;
            } catch (RejectedExecutionException ex) {
                rejected++;
            }
        }

        LOGGER.info("Overload stats: accepted=" + accepted + ", rejected=" + rejected);
        pool.shutdownNow();
    }

    private static void demonstrateShutdown() throws Exception {
        LOGGER.info("=== Demo 3: Graceful shutdown ===");

        PoolConfig config = new PoolConfig(
                2, 4, 5, TimeUnit.SECONDS, 5, 1, RejectionPolicy.ABORT
        );
        CustomThreadPool pool = new CustomThreadPool(config);

        for (int taskId = 1; taskId <= 8; taskId++) {
            pool.execute(sleepingRunnable(taskId, 1000));
        }

        pool.shutdown();
        boolean terminated = pool.awaitTermination(60, TimeUnit.SECONDS);
        LOGGER.info("Graceful shutdown completed: " + terminated
                + ", remaining workers: " + pool.getActiveWorkerCount());
    }

    private static void runBenchmarkComparison() throws Exception {
        LOGGER.info("=== Benchmark: CustomThreadPool vs ThreadPoolExecutor ===");

        int taskCount = 500;
        long taskSleepMs = 10;

        BenchmarkResult customResult = benchmarkCustomPool(taskCount, taskSleepMs);
        BenchmarkResult standardResult = benchmarkStandardPool(taskCount, taskSleepMs);

        LOGGER.info(String.format(
                "CustomThreadPool: throughput=%.2f tasks/sec, avg latency=%.2f ms",
                customResult.throughput(), customResult.avgLatencyMs()));
        LOGGER.info(String.format(
                "ThreadPoolExecutor: throughput=%.2f tasks/sec, avg latency=%.2f ms",
                standardResult.throughput(), standardResult.avgLatencyMs()));
    }

    private static void runParameterResearch() throws Exception {
        LOGGER.info("=== Parameter research ===");

        int taskCount = 200;
        long taskSleepMs = 10;

        LOGGER.info("corePoolSize | maxPoolSize | queueSize | throughput (tasks/sec)");
        int[][] configs = {
                {2, 4, 50},
                {4, 8, 50},
                {8, 16, 50}
        };

        for (int[] cfg : configs) {
            BenchmarkResult result = benchmarkCustomPoolWithConfig(
                    cfg[0], cfg[1], cfg[2], taskCount, taskSleepMs);
            LOGGER.info(String.format(
                    "%12d | %11d | %9d | %18.2f",
                    cfg[0], cfg[1], cfg[2], result.throughput()));
        }
    }

    private static BenchmarkResult benchmarkCustomPool(int taskCount, long taskSleepMs) throws Exception {
        return benchmarkCustomPoolWithConfig(4, 8, 150, taskCount, taskSleepMs);
    }

    private static BenchmarkResult benchmarkCustomPoolWithConfig(
            int core, int max, int queueSize, int taskCount, long taskSleepMs) throws Exception {
        PoolConfig config = new PoolConfig(
                core, max, 60, TimeUnit.SECONDS, queueSize, 0, RejectionPolicy.ABORT
        );
        CustomThreadPool pool = new CustomThreadPool(config);
        return runBenchmark(pool::submit, pool::shutdown, pool::awaitTermination, taskCount, taskSleepMs);
    }

    private static BenchmarkResult benchmarkStandardPool(int taskCount, long taskSleepMs) throws Exception {
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                4, 8, 60, TimeUnit.SECONDS,
                new java.util.concurrent.ArrayBlockingQueue<>(1200),
                new NamedThreadFactory("StdPool"),
                new ThreadPoolExecutor.AbortPolicy()
        );
        return runBenchmark(executor::submit, executor::shutdown, executor::awaitTermination, taskCount, taskSleepMs);
    }

    private static BenchmarkResult runBenchmark(
            TaskSubmitter submitter,
            Runnable shutdown,
            Awaiter awaiter,
            int taskCount,
            long taskSleepMs) throws Exception {
        List<Future<Long>> futures = new ArrayList<>(taskCount);
        long start = System.nanoTime();

        for (int i = 0; i < taskCount; i++) {
            futures.add(submitter.submit(latencyTask(taskSleepMs)));
        }

        long totalLatency = 0;
        for (Future<Long> future : futures) {
            totalLatency += future.get(5, TimeUnit.MINUTES);
        }

        long taskElapsedNanos = System.nanoTime() - start;

        shutdown.run();
        awaiter.await(5, TimeUnit.MINUTES);

        double throughput = taskCount / (taskElapsedNanos / 1_000_000_000.0);
        double avgLatency = totalLatency / (double) taskCount;
        return new BenchmarkResult(throughput, avgLatency);
    }

    private static Callable<String> sleepingTask(int taskId, long sleepMs) {
        return () -> {
            LOGGER.info("Task-" + taskId + " started");
            Thread.sleep(sleepMs);
            LOGGER.info("Task-" + taskId + " finished");
            return "Task-" + taskId + " done";
        };
    }

    private static Runnable sleepingRunnable(int taskId, long sleepMs) {
        return () -> {
            LOGGER.info("Task-" + taskId + " started");
            try {
                Thread.sleep(sleepMs);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
            LOGGER.info("Task-" + taskId + " finished");
        };
    }

    private static Callable<Long> latencyTask(long sleepMs) {
        return () -> {
            long taskStart = System.nanoTime();
            Thread.sleep(sleepMs);
            return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - taskStart);
        };
    }

    @FunctionalInterface
    private interface TaskSubmitter {
        <T> Future<T> submit(Callable<T> callable);
    }

    @FunctionalInterface
    private interface Awaiter {
        boolean await(long timeout, TimeUnit unit) throws InterruptedException;
    }

    private record BenchmarkResult(double throughput, double avgLatencyMs) {
    }
}
