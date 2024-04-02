package io.geekya215.lamination;

public sealed interface Bound<T> permits Bound.Included, Bound.Excluded, Bound.Unbounded {
    static <T> Bound<T> unbound() {
        return new Unbounded<>();
    }

    static <T> Included<T> included(T value) {
        return new Included<>(value);
    }

    static <T> Excluded<T> excluded(T value) {
        return new Excluded<>(value);
    }

    record Included<T>(T value) implements Bound<T> {
    }

    record Excluded<T>(T value) implements Bound<T> {
    }

    record Unbounded<T>() implements Bound<T> {
    }
}
