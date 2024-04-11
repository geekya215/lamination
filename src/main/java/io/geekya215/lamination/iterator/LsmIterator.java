package io.geekya215.lamination.iterator;

import io.geekya215.lamination.Bound;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.Arrays;

public final class LsmIterator implements StorageIterator {
    // Todo
    // use TwoMergeIterator<MergeIterator, MergeIterator> is better than this?
    private final @NotNull TwoMergeIterator<StorageIterator, StorageIterator> iter;
    private final @NotNull Bound<byte[]> end;
    private boolean valid;

    // NOTICE
    // do not call this directly
    public LsmIterator(@NotNull TwoMergeIterator<StorageIterator, StorageIterator> iter, @NotNull Bound<byte[]> end) {
        this.iter = iter;
        this.end = end;
        this.valid = iter.isValid();
    }

    public static LsmIterator create(@NotNull TwoMergeIterator<StorageIterator, StorageIterator> iter, @NotNull Bound<byte[]> end) throws IOException {
        LsmIterator lsmIterator = new LsmIterator(iter, end);
        lsmIterator.skipDeletedValue();
        return lsmIterator;
    }

    @Override
    public byte @NotNull [] key() {
        return iter.key();
    }

    @Override
    public byte @NotNull [] value() {
        return iter.value();
    }

    @Override
    public boolean isValid() {
        return valid;
    }

    @Override
    public void next() throws IOException {
        innerNext();
        skipDeletedValue();
    }

    private void innerNext() throws IOException {
        iter.next();
        if (!iter.isValid()) {
            valid = false;
            return;
        }
        switch (end) {
            case Bound.Included<byte[]>(byte[] key) -> valid = Arrays.compare(iter.key(), key) <= 0;
            case Bound.Excluded<byte[]>(byte[] key) -> valid = Arrays.compare(iter.key(), key) < 0;
            case Bound.Unbounded<byte[]> _ -> {}
        }
    }

    private void skipDeletedValue() throws IOException {
        while (isValid() && iter.value().length == 0) {
            innerNext();
        }
    }
}
