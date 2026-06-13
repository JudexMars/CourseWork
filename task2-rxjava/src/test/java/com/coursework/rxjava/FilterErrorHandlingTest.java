package com.coursework.rxjava;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class FilterErrorHandlingTest {

    @Test
    void predicateErrorIsDeliveredToObserver() throws Exception {
        AtomicReference<Throwable> error = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);

        Observable.<Integer>create(emitter -> {
            emitter.onNext(1);
            emitter.onComplete();
        })
                .filter(value -> {
                    throw new RuntimeException("filter failed");
                })
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
        assertThat(error.get().getMessage()).isEqualTo("filter failed");
    }
}
