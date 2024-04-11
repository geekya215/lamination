package io.geekya215.lamination.iterator;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.Arrays;

public final class TwoMergeIterator<A extends StorageIterator, B extends StorageIterator> implements StorageIterator {
    private final @NotNull A a;
    private final @NotNull B b;
    private boolean chooseA;

    public TwoMergeIterator(@NotNull A a, @NotNull B b) {
        this.a = a;
        this.b = b;
    }

    static <A extends StorageIterator, B extends StorageIterator> boolean chooseA(A a, B b) {
        if (!a.isValid()) {
            return false;
        }

        if (!b.isValid()) {
            return true;
        }

        return Arrays.compare(a.key(), b.key()) < 0;
    }

    public static <A extends StorageIterator, B extends StorageIterator> TwoMergeIterator<A, B> create(A a, B b) throws IOException {
        TwoMergeIterator<A, B> iter = new TwoMergeIterator<>(a, b);
        iter.skipB();
        iter.chooseA = chooseA(iter.a, iter.b);
        return iter;
    }

    public void skipB() throws IOException {
        if (a.isValid() && b.isValid() && Arrays.equals(a.key(), b.key())) {
            b.next();
        }
    }

    @Override
    public byte @NotNull [] key() {
        return chooseA ? a.key() : b.key();
    }

    @Override
    public byte @NotNull [] value() {
        return chooseA ? a.value() : b.value();
    }

    @Override
    public boolean isValid() {
        return chooseA ? a.isValid() : b.isValid();
    }

    @Override
    public void next() throws IOException {
        if (chooseA) {
            a.next();
        } else {
            b.next();
        }
        skipB();
        chooseA = chooseA(a, b);
    }
}
