package com.coursework.rxjava;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Predicate;

public class Observable<T> {

    @FunctionalInterface
    interface Source<T> {
        void subscribe(Observer<T> observer, Disposable subscription);
    }

    private final Source<T> source;
    private final Scheduler subscribeScheduler;
    private final Scheduler observeScheduler;

    protected Observable(Source<T> source, Scheduler subscribeScheduler, Scheduler observeScheduler) {
        this.source = source;
        this.subscribeScheduler = subscribeScheduler;
        this.observeScheduler = observeScheduler;
    }

    public static <T> Observable<T> create(ObservableOnSubscribe<T> onSubscribe) {
        return new Observable<>((observer, subscription) -> {
            ObservableOnSubscribe.Emitter<T> emitter = new ObservableOnSubscribe.Emitter<T>() {
                @Override
                public void onNext(T item) {
                    if (!subscription.isDisposed()) {
                        observer.onNext(item);
                    }
                }

                @Override
                public void onError(Throwable t) {
                    if (!subscription.isDisposed()) {
                        observer.onError(t);
                    }
                }

                @Override
                public void onComplete() {
                    if (!subscription.isDisposed()) {
                        observer.onComplete();
                    }
                }
            };
            try {
                onSubscribe.subscribe(emitter);
            } catch (Throwable t) {
                observer.onError(t);
            }
        }, null, null);
    }

    public Disposable subscribe(Observer<T> observer) {
        SerialDisposable disposable = new SerialDisposable();
        Observer<T> target = new SafeObserver<>(observer, disposable);

        if (observeScheduler != null) {
            target = wrapWithObserveOn(target, observeScheduler, disposable);
        }

        Observer<T> finalTarget = target;
        Runnable subscribeAction = () -> {
            CompositeDisposable subscription = new CompositeDisposable();
            disposable.set(subscription);
            source.subscribe(finalTarget, subscription);
        };

        if (subscribeScheduler != null) {
            subscribeScheduler.execute(subscribeAction);
        } else {
            subscribeAction.run();
        }

        return disposable;
    }

    public <R> Observable<R> map(Function<T, R> mapper) {
        return new Observable<>((observer, subscription) -> {
            Disposable upstream = Observable.this.subscribe(new Observer<T>() {
                @Override
                public void onNext(T item) {
                    if (subscription.isDisposed()) {
                        return;
                    }
                    try {
                        observer.onNext(mapper.apply(item));
                    } catch (Throwable t) {
                        observer.onError(t);
                    }
                }

                @Override
                public void onError(Throwable t) {
                    observer.onError(t);
                }

                @Override
                public void onComplete() {
                    observer.onComplete();
                }
            });
            addToSubscription(subscription, upstream);
        }, subscribeScheduler, observeScheduler);
    }

    public Observable<T> filter(Predicate<T> predicate) {
        return new Observable<>((observer, subscription) -> {
            Disposable upstream = Observable.this.subscribe(new Observer<T>() {
                @Override
                public void onNext(T item) {
                    if (subscription.isDisposed()) {
                        return;
                    }
                    try {
                        if (predicate.test(item)) {
                            observer.onNext(item);
                        }
                    } catch (Throwable t) {
                        observer.onError(t);
                    }
                }

                @Override
                public void onError(Throwable t) {
                    observer.onError(t);
                }

                @Override
                public void onComplete() {
                    observer.onComplete();
                }
            });
            addToSubscription(subscription, upstream);
        }, subscribeScheduler, observeScheduler);
    }

    public <R> Observable<R> flatMap(Function<T, Observable<R>> mapper) {
        return new Observable<>((observer, subscription) -> {
            AtomicInteger activeInners = new AtomicInteger(0);
            AtomicBoolean upstreamCompleted = new AtomicBoolean(false);
            AtomicBoolean terminated = new AtomicBoolean(false);

            Disposable upstream = Observable.this.subscribe(new Observer<T>() {
                @Override
                public void onNext(T item) {
                    if (subscription.isDisposed() || terminated.get()) {
                        return;
                    }
                    try {
                        Observable<R> inner = mapper.apply(item);
                        activeInners.incrementAndGet();

                        Disposable innerDisposable = inner.subscribe(new Observer<R>() {
                            @Override
                            public void onNext(R value) {
                                if (!subscription.isDisposed() && !terminated.get()) {
                                    observer.onNext(value);
                                }
                            }

                            @Override
                            public void onError(Throwable t) {
                                terminateWithError(t);
                            }

                            @Override
                            public void onComplete() {
                                if (activeInners.decrementAndGet() == 0 && upstreamCompleted.get()) {
                                    terminateWithComplete();
                                }
                            }
                        });
                        addToSubscription(subscription, innerDisposable);
                    } catch (Throwable t) {
                        terminateWithError(t);
                    }
                }

                @Override
                public void onError(Throwable t) {
                    terminateWithError(t);
                }

                @Override
                public void onComplete() {
                    upstreamCompleted.set(true);
                    if (activeInners.get() == 0) {
                        terminateWithComplete();
                    }
                }

                private void terminateWithError(Throwable t) {
                    if (terminated.compareAndSet(false, true)) {
                        subscription.dispose();
                        observer.onError(t);
                    }
                }

                private void terminateWithComplete() {
                    if (terminated.compareAndSet(false, true)) {
                        observer.onComplete();
                    }
                }
            });
            addToSubscription(subscription, upstream);
        }, subscribeScheduler, observeScheduler);
    }

    public Observable<T> subscribeOn(Scheduler scheduler) {
        return new Observable<>(source, scheduler, observeScheduler);
    }

    public Observable<T> observeOn(Scheduler scheduler) {
        return new Observable<>(source, subscribeScheduler, scheduler);
    }

    private static void addToSubscription(Disposable subscription, Disposable upstream) {
        if (subscription instanceof CompositeDisposable composite) {
            composite.add(upstream);
        }
    }

    private Observer<T> wrapWithObserveOn(Observer<T> observer, Scheduler scheduler, Disposable disposable) {
        return new Observer<T>() {
            @Override
            public void onNext(T item) {
                if (disposable.isDisposed()) {
                    return;
                }
                scheduler.execute(() -> {
                    if (!disposable.isDisposed()) {
                        observer.onNext(item);
                    }
                });
            }

            @Override
            public void onError(Throwable t) {
                if (disposable.isDisposed()) {
                    return;
                }
                scheduler.execute(() -> {
                    if (!disposable.isDisposed()) {
                        observer.onError(t);
                    }
                });
            }

            @Override
            public void onComplete() {
                if (disposable.isDisposed()) {
                    return;
                }
                scheduler.execute(() -> {
                    if (!disposable.isDisposed()) {
                        observer.onComplete();
                    }
                });
            }
        };
    }
}
