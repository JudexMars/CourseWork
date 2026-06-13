package com.coursework.rxjava;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class FlatMapOperatorTest {

    @Test
    void flatMapMergesInnerStreams() throws Exception {
        List<String> values = new ArrayList<>();
        CountDownLatch latch = new CountDownLatch(1);

        Observable.<String>create(emitter -> {
            emitter.onNext("a");
            emitter.onNext("b");
            emitter.onComplete();
        })
                .flatMap(prefix -> Observable.<String>create(inner -> {
                    inner.onNext(prefix + "-1");
                    inner.onNext(prefix + "-2");
                    inner.onComplete();
                }))
                .subscribe(new Observer<String>() {
                    @Override
                    public void onNext(String item) {
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

        assertThat(latch.await(3, TimeUnit.SECONDS)).isTrue();
        assertThat(values).containsExactlyInAnyOrder("a-1", "a-2", "b-1", "b-2");
    }
}
