package com.coursework.task1;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class NamedThreadFactory implements ThreadFactory {

    private final String prefix;
    private final AtomicInteger threadNumber = new AtomicInteger(1);

    public NamedThreadFactory(String prefix) {
        this.prefix = prefix;
    }

    @Override
    public Thread newThread(Runnable runnable) {
        String name = prefix + "-worker-" + threadNumber.getAndIncrement();
        PoolLogger.threadFactory("Creating new thread: " + name);
        Thread thread = new Thread(runnable, name);
        thread.setDaemon(false);
        return thread;
    }
}
