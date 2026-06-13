package com.coursework.rxjava;

import java.util.concurrent.atomic.AtomicBoolean;

final class SafeObserver<T> implements Observer<T> {

    private final Observer<T> downstream;
    private final AtomicBoolean terminated = new AtomicBoolean(false);
    private final Disposable disposable;

    SafeObserver(Observer<T> downstream, Disposable disposable) {
        this.downstream = downstream;
        this.disposable = disposable;
    }

    @Override
    public void onNext(T item) {
        if (isInactive()) {
            return;
        }
        try {
            downstream.onNext(item);
        } catch (Throwable t) {
            onError(t);
        }
    }

    @Override
    public void onError(Throwable t) {
        if (!terminated.compareAndSet(false, true)) {
            return;
        }
        if (disposable.isDisposed()) {
            return;
        }
        downstream.onError(t);
    }

    @Override
    public void onComplete() {
        if (!terminated.compareAndSet(false, true)) {
            return;
        }
        if (disposable.isDisposed()) {
            return;
        }
        downstream.onComplete();
    }

    private boolean isInactive() {
        return terminated.get() || disposable.isDisposed();
    }
}
