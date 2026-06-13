package com.coursework.rxjava;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

class ErrorHandlingTest {

    @Test
    void sourceErrorIsDeliveredToObserver() throws Exception {
        AtomicReference<Throwable> error = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);

        Observable.<Object>create(emitter -> emitter.onError(new IllegalStateException("boom")))
                .subscribe(new Observer<Object>() {
                    @Override
                    public void onNext(Object item) {
                    }

                    @Override
                    public void onError(Throwable t) {
                        error.set(t);
                        latch.countDown();
                    }

                    @Override
                    public void onComplete() {
                        latch.countDown();
                    }
                });

        assertThat(latch.await(2, TimeUnit.SECONDS)).isTrue();
        assertThat(error.get()).isInstanceOf(IllegalStateException.class);
        assertThat(error.get().getMessage()).isEqualTo("boom");
    }

    @Test
    void mapErrorIsDeliveredToObserver() throws Exception {
        AtomicReference<Throwable> error = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);

        Function<Integer, Integer> failingMapper = value -> {
            throw new RuntimeException("map failed");
        };

        Observable.<Integer>create(emitter -> {
            emitter.onNext(1);
            emitter.onComplete();
        })
                .map(failingMapper)
                .subscribe(new Observer<Integer>() {
                    @Override
                    public void onNext(Integer item) {
                    }

                    @Override
                    public void onError(Throwable t) {
                        error.set(t);
                        latch.countDown();
                    }

                    @Override
                    public void onComplete() {
                        latch.countDown();
                    }
                });

        assertThat(latch.await(2, TimeUnit.SECONDS)).isTrue();
        assertThat(error.get()).isInstanceOf(RuntimeException.class);
        assertThat(error.get().getMessage()).isEqualTo("map failed");
    }

    @Test
    void onErrorIsCalledOnlyOnce() throws Exception {
        AtomicInteger errorCount = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(1);

        Observable.<Integer>create(emitter -> {
            emitter.onError(new RuntimeException("first"));
            emitter.onError(new RuntimeException("second"));
        }).subscribe(new Observer<Integer>() {
            @Override
            public void onNext(Integer item) {
            }

            @Override
            public void onError(Throwable t) {
                errorCount.incrementAndGet();
                latch.countDown();
            }

            @Override
            public void onComplete() {
                latch.countDown();
            }
        });

        assertThat(latch.await(2, TimeUnit.SECONDS)).isTrue();
        assertThat(errorCount.get()).isEqualTo(1);
    }
}
