package com.coursework.rxjava;

@FunctionalInterface
public interface ObservableOnSubscribe<T> {
    void subscribe(Emitter<T> emitter) throws Exception;

    interface Emitter<T> {
        void onNext(T item);

        void onError(Throwable t);

        void onComplete();
    }
}
