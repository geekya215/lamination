package io.geekya215.lamination.tuple;

public record Tuple2<T1, T2>(T1 t1, T2 t2) {
    static <T1, T2> Tuple2<T1, T2> of(T1 t1, T2 t2) {
        return new Tuple2<>(t1, t2);
    }
}
