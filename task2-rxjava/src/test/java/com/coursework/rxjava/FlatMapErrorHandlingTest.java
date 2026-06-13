package com.coursework.rxjava;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class FlatMapErrorHandlingTest {

    @Test
    void innerStreamErrorIsDeliveredToObserver() throws Exception {
        AtomicReference<Throwable> error = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);

        Observable.<Integer>create(emitter -> {
            emitter.onNext(1);
            emitter.onComplete();
        })
                .flatMap(value -> Observable.<String>create(inner ->
                        inner.onError(new IllegalArgumentException("inner failed"))))
                .subscribe(new Observer<String>() {
                    @Override
                    public void onNext(String item) {
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

        assertThat(latch.await(3, TimeUnit.SECONDS)).isTrue();
        assertThat(error.get()).isInstanceOf(IllegalArgumentException.class);
        assertThat(error.get().getMessage()).isEqualTo("inner failed");
    }
}
