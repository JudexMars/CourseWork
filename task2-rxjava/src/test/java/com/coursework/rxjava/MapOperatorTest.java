package com.coursework.rxjava;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class MapOperatorTest {

    @Test
    void mapTransformsValues() throws Exception {
        List<String> values = new ArrayList<>();
        CountDownLatch latch = new CountDownLatch(1);

        Observable.<Integer>create(emitter -> {
            emitter.onNext(1);
            emitter.onNext(2);
            emitter.onComplete();
        })
                .map(value -> "num-" + value)
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

        assertThat(latch.await(2, TimeUnit.SECONDS)).isTrue();
        assertThat(values).containsExactly("num-1", "num-2");
    }
}
