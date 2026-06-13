package com.coursework.rxjava;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class DisposableTest {

    @Test
    void disposeStopsEmission() throws Exception {
        List<Integer> values = new ArrayList<>();
        CountDownLatch firstValue = new CountDownLatch(1);
        AtomicReference<Disposable> disposableRef = new AtomicReference<>();

        Disposable disposable = Observable.<Integer>create(emitter -> {
            for (int i = 1; i <= 10; i++) {
                emitter.onNext(i);
                Thread.sleep(50);
            }
            emitter.onComplete();
        })
                .subscribeOn(Schedulers.single())
                .subscribe(new Observer<Integer>() {
                    @Override
                    public void onNext(Integer item) {
                        values.add(item);
                        if (item == 1) {
                            disposableRef.get().dispose();
                            firstValue.countDown();
                        }
                    }

                    @Override
                    public void onError(Throwable t) {
                    }

                    @Override
                    public void onComplete() {
                    }
                });

        disposableRef.set(disposable);
        assertThat(firstValue.await(3, TimeUnit.SECONDS)).isTrue();
        Thread.sleep(300);
        assertThat(disposable.isDisposed()).isTrue();
        assertThat(values).containsExactly(1);
    }

    @Test
    void isDisposedReflectsState() {
        Disposable disposable = Observable.<Object>create(emitter -> {
        }).subscribe(new Observer<Object>() {
            @Override
            public void onNext(Object item) {
            }

            @Override
            public void onError(Throwable t) {
            }

            @Override
            public void onComplete() {
            }
        });

        assertThat(disposable.isDisposed()).isFalse();
        disposable.dispose();
        assertThat(disposable.isDisposed()).isTrue();
    }
}
