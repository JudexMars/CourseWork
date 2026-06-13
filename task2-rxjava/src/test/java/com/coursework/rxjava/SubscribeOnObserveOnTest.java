package com.coursework.rxjava;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class SubscribeOnObserveOnTest {

    @Test
    void subscribeOnRunsSourceOnSchedulerThread() throws Exception {
        AtomicReference<String> sourceThread = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);

        Observable.<Integer>create(emitter -> {
            sourceThread.set(Thread.currentThread().getName());
            emitter.onNext(1);
            emitter.onComplete();
        })
                .subscribeOn(Schedulers.io())
                .subscribe(new Observer<Integer>() {
                    @Override
                    public void onNext(Integer item) {
                    }

                    @Override
                    public void onError(Throwable t) {
                        latch.countDown();
                    }

                    @Override
                    public void onComplete() {
                        latch.countDown();
                    }
                });

        assertThat(latch.await(3, TimeUnit.SECONDS)).isTrue();
        assertThat(sourceThread.get()).startsWith("rx-io-");
    }

    @Test
    void observeOnDeliversEventsOnSchedulerThread() throws Exception {
        AtomicReference<String> observeThread = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);

        Observable.<Integer>create(emitter -> {
            emitter.onNext(1);
            emitter.onComplete();
        })
                .observeOn(Schedulers.computation())
                .subscribe(new Observer<Integer>() {
                    @Override
                    public void onNext(Integer item) {
                        observeThread.set(Thread.currentThread().getName());
                    }

                    @Override
                    public void onError(Throwable t) {
                        latch.countDown();
                    }

                    @Override
                    public void onComplete() {
                        latch.countDown();
                    }
                });

        assertThat(latch.await(3, TimeUnit.SECONDS)).isTrue();
        assertThat(observeThread.get()).startsWith("rx-computation-");
    }
}
