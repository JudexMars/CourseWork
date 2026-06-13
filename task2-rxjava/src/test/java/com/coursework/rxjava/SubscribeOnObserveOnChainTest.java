package com.coursework.rxjava;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class SubscribeOnObserveOnChainTest {

    @Test
    void subscribeOnAndObserveOnRunOnDifferentSchedulerThreads() throws Exception {
        AtomicReference<String> sourceThread = new AtomicReference<>();
        AtomicReference<String> observeThread = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);

        Observable.<Integer>create(emitter -> {
            sourceThread.set(Thread.currentThread().getName());
            emitter.onNext(42);
            emitter.onComplete();
        })
                .subscribeOn(Schedulers.io())
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
        assertThat(sourceThread.get()).startsWith("rx-io-");
        assertThat(observeThread.get()).startsWith("rx-computation-");
        assertThat(sourceThread.get()).isNotEqualTo(observeThread.get());
    }
}
