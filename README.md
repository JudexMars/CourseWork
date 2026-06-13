# Курсовая работа: Многопоточное и асинхронное программирование на Java

Курсовая работа состоит из двух заданий по дисциплине «Многопоточное и асинхронное программирование на Java».

## Структура проекта

```
CourseWork/
├── task1-threadpool/     # Задание 1: кастомный Thread Pool
│   ├── src/main/java/com/coursework/task1/
│   └── REPORT.md
├── task2-rxjava/         # Задание 2: мини-RxJava
│   ├── src/main/java/com/coursework/rxjava/
│   ├── src/test/java/com/coursework/rxjava/
│   └── REPORT.md
├── build.gradle.kts
└── settings.gradle.kts
```

- **Java 21**, **Gradle 8.12** (multi-module)
- Задание 1: демонстрационная программа (без unit-тестов)
- Задание 2: JUnit 5 + AssertJ

## Задание 1: Кастомный Thread Pool

Реализован пул потоков с Round Robin балансировкой, per-worker очередями, `minSpareThreads`, политикой отказа ABORT и подробным логированием.

**Отчёт:** [task1-threadpool/REPORT.md](task1-threadpool/REPORT.md)

```bash
./gradlew :task1-threadpool:run
```

## Задание 2: Мини-RxJava

Реализованы `Observer`, `Observable`, операторы `map`/`filter`/`flatMap`, `Scheduler` (IO/Computation/Single), `subscribeOn`/`observeOn`, `Disposable` и unit-тесты.

**Отчёт:** [task2-rxjava/REPORT.md](task2-rxjava/REPORT.md)

```bash
./gradlew :task2-rxjava:test
```

## Сборка всего проекта

```bash
./gradlew build
```

## Требования

- JDK 21+
- Gradle (wrapper включён: `./gradlew`)
