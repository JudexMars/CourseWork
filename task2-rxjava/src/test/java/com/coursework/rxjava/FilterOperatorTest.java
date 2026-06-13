package com.coursework.rxjava;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class FilterOperatorTest {

    @Test
    void filterKeepsOnlyMatchingItems() throws Exception {
        List<Integer> values = new ArrayList<>();
        CountDownLatch latch = new CountDownLatch(1);

        Observable.<Integer>create(emitter -> {
            emitter.onNext(1);
            emitter.onNext(2);
            emitter.onNext(3);
            emitter.onNext(4);
            emitter.onComplete();
        })
                .filter(value -> value % 2 == 0)
                .subscribe(new Observer<Integer>() {
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
                        latch.countDown();
                    }
                });

        assertThat(latch.await(2, TimeUnit.SECONDS)).isTrue();
        assertThat(values).containsExactly(2, 4);
    }

    @Test
    void filterCanProduceEmptyResult() throws Exception {
        List<Integer> values = new ArrayList<>();
        CountDownLatch latch = new CountDownLatch(1);

        Observable.<Integer>create(emitter -> {
            emitter.onNext(1);
            emitter.onNext(3);
            emitter.onComplete();
        })
                .filter(value -> value % 2 == 0)
                .subscribe(new Observer<Integer>() {
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
                        latch.countDown();
                    }
                });

        assertThat(latch.await(2, TimeUnit.SECONDS)).isTrue();
        assertThat(values).isEmpty();
    }
}
