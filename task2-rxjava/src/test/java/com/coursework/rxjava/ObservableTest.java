package com.coursework.rxjava;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

class ObservableTest {

    @Test
    void createEmitsItemsAndCompletes() throws Exception {
        List<Integer> values = new ArrayList<>();
        AtomicBoolean completed = new AtomicBoolean(false);
        CountDownLatch latch = new CountDownLatch(1);

        Observable.<Integer>create(emitter -> {
            emitter.onNext(1);
            emitter.onNext(2);
            emitter.onComplete();
        }).subscribe(new Observer<Integer>() {
            @Override
            public void onNext(Integer item) {
                values.add(item);
            }

            @Override
            public void onError(Throwable t) {
                latch.countDown();
            }

            @Override
            public void onComplete() {
                completed.set(true);
                latch.countDown();
            }
        });

        assertThat(latch.await(2, TimeUnit.SECONDS)).isTrue();
        assertThat(values).containsExactly(1, 2);
        assertThat(completed).isTrue();
    }
}
