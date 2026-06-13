package com.coursework.rxjava;

public interface Scheduler {
    void execute(Runnable task);
}
