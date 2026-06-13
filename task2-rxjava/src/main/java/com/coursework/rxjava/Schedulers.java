package com.coursework.rxjava;

public final class Schedulers {

    private Schedulers() {
    }

    public static Scheduler io() {
        return IOThreadScheduler.getInstance();
    }

    public static Scheduler computation() {
        return ComputationScheduler.getInstance();
    }

    public static Scheduler single() {
        return SingleThreadScheduler.getInstance();
    }
}
