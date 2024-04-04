package io.geekya215.lamination;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.*;

public final class MergeIterator implements StorageIterator {
    private final @NotNull TreeMap<byte[], byte[]> map;
    private final @NotNull Iterator<Map.Entry<byte[], byte[]>> iter;
    private @NotNull Map.Entry<byte[], byte[]> current;

    public MergeIterator(@NotNull TreeMap<byte[], byte[]> map) {
        this.map = map;
        this.iter = map.entrySet().iterator();
        this.current = iter.hasNext() ? iter.next() : EMPTY_ENTRY;
    }

    // Todo
    // use priority queue for lazy evaluate
    public static @NotNull MergeIterator create(@NotNull List<StorageIterator> iters) throws IOException {
        TreeMap<byte[], byte[]> map = new TreeMap<>(Arrays::compare);
        for (StorageIterator iter : iters) {
            while (iter.isValid()) {
                map.put(iter.key(), iter.value());
                iter.next();
            }
        }
        return new MergeIterator(map);
    }

    @Override
    public byte @NotNull [] key() {
        return current.getKey();
    }

    @Override
    public byte @NotNull [] value() {
        return current.getValue();
    }

    @Override
    public boolean isValid() {
        return current.getKey().length != 0;
    }

    @Override
    public void next() {
        current = iter.hasNext() ? iter.next() : EMPTY_ENTRY;
    }
}
