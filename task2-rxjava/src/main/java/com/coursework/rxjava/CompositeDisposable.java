package com.coursework.rxjava;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

final class CompositeDisposable implements Disposable {

    private final List<Disposable> disposables = new ArrayList<>();
    private final AtomicBoolean disposed = new AtomicBoolean(false);

    synchronized void add(Disposable disposable) {
        if (!disposed.get()) {
            disposables.add(disposable);
        }
    }

    @Override
    public synchronized void dispose() {
        if (disposed.compareAndSet(false, true)) {
            for (Disposable disposable : disposables) {
                disposable.dispose();
            }
            disposables.clear();
        }
    }

    @Override
    public boolean isDisposed() {
        return disposed.get();
    }
}
