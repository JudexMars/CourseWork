package com.coursework.task1;

import java.util.concurrent.TimeUnit;

public record PoolConfig(
        int corePoolSize,
        int maxPoolSize,
        long keepAliveTime,
        TimeUnit timeUnit,
        int queueSize,
        int minSpareThreads,
        RejectionPolicy rejectionPolicy
) {
    public PoolConfig {
        if (corePoolSize < 0) {
            throw new IllegalArgumentException("corePoolSize must be >= 0");
        }
        if (maxPoolSize < corePoolSize) {
            throw new IllegalArgumentException("maxPoolSize must be >= corePoolSize");
        }
        if (queueSize <= 0) {
            throw new IllegalArgumentException("queueSize must be > 0");
        }
        if (minSpareThreads < 0) {
            throw new IllegalArgumentException("minSpareThreads must be >= 0");
        }
        if (keepAliveTime < 0) {
            throw new IllegalArgumentException("keepAliveTime must be >= 0");
        }
        if (timeUnit == null) {
            throw new IllegalArgumentException("timeUnit must not be null");
        }
        if (rejectionPolicy == null) {
            throw new IllegalArgumentException("rejectionPolicy must not be null");
        }
    }
}
