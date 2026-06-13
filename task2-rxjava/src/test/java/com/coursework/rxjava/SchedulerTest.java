package com.coursework.rxjava;

import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class SchedulerTest {

    @Test
    void ioSchedulerRunsTasksInDifferentThreads() throws Exception {
        Set<String> threads = ConcurrentHashMap.newKeySet();
        CountDownLatch latch = new CountDownLatch(3);

        Scheduler scheduler = Schedulers.io();
        for (int i = 0; i < 3; i++) {
            scheduler.execute(() -> {
                threads.add(Thread.currentThread().getName());
                latch.countDown();
            });
        }

        assertThat(latch.await(3, TimeUnit.SECONDS)).isTrue();
        assertThat(threads).hasSizeGreaterThan(1);
        assertThat(threads).anyMatch(name -> name.startsWith("rx-io-"));
    }

    @Test
    void computationSchedulerUsesNamedThreads() throws Exception {
        String[] threadName = new String[1];
        CountDownLatch latch = new CountDownLatch(1);

        Schedulers.computation().execute(() -> {
            threadName[0] = Thread.currentThread().getName();
            latch.countDown();
        });

        assertThat(latch.await(2, TimeUnit.SECONDS)).isTrue();
        assertThat(threadName[0]).startsWith("rx-computation-");
    }

    @Test
    void singleSchedulerUsesOneThread() throws Exception {
        Set<String> threads = ConcurrentHashMap.newKeySet();
        CountDownLatch latch = new CountDownLatch(3);

        Scheduler scheduler = Schedulers.single();
        for (int i = 0; i < 3; i++) {
            scheduler.execute(() -> {
                threads.add(Thread.currentThread().getName());
                latch.countDown();
            });
        }

        assertThat(latch.await(3, TimeUnit.SECONDS)).isTrue();
        assertThat(threads).hasSize(1);
        assertThat(threads.iterator().next()).startsWith("rx-single-");
    }
}
