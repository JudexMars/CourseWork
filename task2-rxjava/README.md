# Задание 2: Мини-RxJava — отчёт

## 1. Архитектура системы

Реализована упрощённая реактивная библиотека по паттерну Observer с ленивой композицией операторов.

```
Observable<T>
    ├── create(ObservableOnSubscribe)
    ├── subscribe(Observer) → Disposable
    ├── map / filter / flatMap
    ├── subscribeOn(Scheduler)
    └── observeOn(Scheduler)

Observer<T>     — onNext, onError, onComplete
Disposable      — dispose(), isDisposed()
Scheduler       — execute(Runnable)
```

### Поток данных

1. Подписчик вызывает `subscribe(observer)`.
2. Создаётся `SerialDisposable` → `CompositeDisposable` для цепочки подписок.
3. Операторы образуют цепочку: каждый возвращает новый `Observable`, оборачивающий upstream.
4. `SafeObserver` гарантирует единственный вызов `onError` / `onComplete`.
5. При `dispose()` отменяются все upstream-подписки в `CompositeDisposable`.

### Ключевые классы

| Класс | Назначение |
|-------|-----------|
| `Observable` | Источник данных, операторы, подписка |
| `Observer` | Потребитель событий |
| `ObservableOnSubscribe` | Фабрика для `create()` |
| `Disposable` / `SerialDisposable` / `CompositeDisposable` | Управление жизненным циклом подписки |
| `SafeObserver` | Защита от повторных terminal-событий |
| `Scheduler` + реализации | Управление потоками выполнения |
| `Schedulers` | Фабрика: `io()`, `computation()`, `single()` |

---

## 2. Schedulers — принципы и применение

### IOThreadScheduler (`Schedulers.io()`)

- Основа: `Executors.newCachedThreadPool()`
- Потоки создаются по требованию, неограниченный пул (с кэшированием и reuse)
- **Применение**: блокирующие IO-операции (сеть, файлы, БД)
- **Не использовать** для CPU-intensive вычислений — создаст слишком много потоков

### ComputationScheduler (`Schedulers.computation()`)

- Основа: `Executors.newFixedThreadPool(availableProcessors())`
- Число потоков = количество ядер CPU
- **Применение**: вычисления, трансформации данных, парсинг
- **Не использовать** для блокирующих операций — заблокирует весь пул

### SingleThreadScheduler (`Schedulers.single()`)

- Основа: `Executors.newSingleThreadExecutor()`
- Все задачи выполняются последовательно в одном потоке
- **Применение**: операции, требующие строгого порядка (запись в UI, логирование)

### subscribeOn vs observeOn

| Метод | Что переносит | Когда вызывается |
|-------|---------------|------------------|
| `subscribeOn` | Подписку на upstream (вызов `subscribe()`) | Один раз при подписке |
| `observeOn` | Доставку `onNext/onError/onComplete` подписчику | На каждое событие |

Типичный паттерн:

```java
Observable.<Data>create(source -> fetchFromNetwork(source))
    .subscribeOn(Schedulers.io())        // fetch в IO-потоке
    .map(this::parse)
    .observeOn(Schedulers.computation()) // parse в computation
    .subscribe(observer);
```

---

## 3. Операторы

### map

Преобразует каждый элемент: `T → R`. Ошибка в mapper → `onError`.

```java
Observable.<Integer>create(e -> { e.onNext(1); e.onNext(2); e.onComplete(); })
    .map(n -> n * 10)
    .subscribe(observer); // получит 10, 20
```

### filter

Пропускает элементы, удовлетворяющие предикату.

```java
Observable.<Integer>create(e -> { e.onNext(1); e.onNext(2); e.onComplete(); })
    .filter(n -> n % 2 == 0)
    .subscribe(observer); // получит только 2
```

### flatMap

Для каждого элемента создаёт внутренний `Observable` и мержит результаты. Завершение downstream — когда upstream завершён и все inner-потоки завершены.

```java
Observable.<String>create(e -> { e.onNext("a"); e.onComplete(); })
    .flatMap(s -> Observable.<String>create(inner -> {
        inner.onNext(s + "-1");
        inner.onComplete();
    }))
    .subscribe(observer); // получит "a-1"
```

---

## 4. Обработка ошибок и отмена

- Исключение в source, mapper или predicate → `onError(t)`, поток завершается
- `SafeObserver` гарантирует: после `onError` или `onComplete` последующие события игнорируются
- `dispose()` устанавливает флаг; emitter проверяет `subscription.isDisposed()` перед доставкой
- `flatMap` при ошибке во inner-потоке отменяет все активные подписки

---

## 5. Тестирование

### Запуск

```bash
./gradlew :task2-rxjava:test
```

### Покрытие (18 тестов)

| Класс | Сценарии |
|-------|----------|
| `ObservableTest` | `create()`, базовая подписка, `onComplete` |
| `MapOperatorTest` | Преобразование элементов |
| `FilterOperatorTest` | Фильтрация, пустой результат |
| `FilterErrorHandlingTest` | Ошибка в `Predicate` → `onError` |
| `FlatMapOperatorTest` | Вложенные Observable |
| `FlatMapErrorHandlingTest` | Ошибка во inner-потоке → `onError` |
| `SchedulerTest` | IO / Computation / Single — разные потоки |
| `SubscribeOnObserveOnTest` | Проверка потока выполнения по отдельности |
| `SubscribeOnObserveOnChainTest` | Цепочка `subscribeOn` + `observeOn` в разных потоках |
| `ErrorHandlingTest` | Ошибка в source/map, единственный `onError` |
| `DisposableTest` | `dispose()` останавливает эмиссию |

### Пример теста subscribeOn

```java
Observable.<Integer>create(emitter -> {
    sourceThread.set(Thread.currentThread().getName());
    emitter.onNext(1);
    emitter.onComplete();
})
.subscribeOn(Schedulers.io())
.subscribe(observer);

assertThat(sourceThread.get()).startsWith("rx-io-");
```

---

## 6. Примеры использования

### Базовая подписка

```java
Observable.<String>create(emitter -> {
    emitter.onNext("Hello");
    emitter.onNext("RxJava");
    emitter.onComplete();
}).subscribe(new Observer<String>() {
    public void onNext(String item) { System.out.println(item); }
    public void onError(Throwable t) { t.printStackTrace(); }
    public void onComplete() { System.out.println("Done"); }
});
```

### Цепочка операторов

```java
Observable.<Integer>create(emitter -> {
    for (int i = 1; i <= 5; i++) emitter.onNext(i);
    emitter.onComplete();
})
.map(n -> n * n)
.filter(n -> n > 4)
.subscribe(observer); // 9, 16, 25
```

### Отмена подписки

```java
Disposable d = Observable.<Integer>create(emitter -> {
    for (int i = 1; i <= 100; i++) {
        emitter.onNext(i);
        Thread.sleep(50);
    }
    emitter.onComplete();
})
.subscribeOn(Schedulers.single())
.subscribe(observer);

// через 100ms:
d.dispose();
```
