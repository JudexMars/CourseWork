package com.coursework.rxjava;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class ComputationScheduler implements Scheduler {

    private static final ComputationScheduler INSTANCE = new ComputationScheduler();
    private final ExecutorService executor = Executors.newFixedThreadPool(
            Runtime.getRuntime().availableProcessors(),
            new NamedFactory("rx-computation")
    );

    private ComputationScheduler() {
    }

    public static ComputationScheduler getInstance() {
        return INSTANCE;
    }

    @Override
    public void execute(Runnable task) {
        executor.execute(task);
    }

    private static final class NamedFactory implements ThreadFactory {
        private final String prefix;
        private final AtomicInteger counter = new AtomicInteger(1);

        private NamedFactory(String prefix) {
            this.prefix = prefix;
        }

        @Override
        public Thread newThread(Runnable runnable) {
            return new Thread(runnable, prefix + "-" + counter.getAndIncrement());
        }
    }
}
