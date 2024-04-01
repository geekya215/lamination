package io.geekya215.lamination;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;

public final class MemoryTable {
    static final Comparator<byte[]> DEFAULT_COMPARATOR = Arrays::compare;
    private final int id;
    private final @NotNull ConcurrentSkipListMap<byte[], byte[]> skipList;
    private final @Nullable WriteAheadLog wal;
    private final @NotNull AtomicInteger approximateSize;

    public MemoryTable(
            int id,
            @NotNull ConcurrentSkipListMap<byte[], byte[]> skipList,
            @Nullable WriteAheadLog wal,
            @NotNull AtomicInteger approximateSize) {
        this.id = id;
        this.skipList = skipList;
        this.wal = wal;
        this.approximateSize = approximateSize;
    }

    public static @NotNull MemoryTable create(int id) {
        return new MemoryTable(id, new ConcurrentSkipListMap<>(DEFAULT_COMPARATOR), null, new AtomicInteger());
    }

    public static @NotNull MemoryTable createWithWAL(int id, @NotNull Path path) throws FileNotFoundException {
        return new MemoryTable(id, new ConcurrentSkipListMap<>(DEFAULT_COMPARATOR), WriteAheadLog.create(path), new AtomicInteger());
    }

    public static @NotNull MemoryTable recoverFromWAL(int id, @NotNull Path path) throws IOException {
        ConcurrentSkipListMap<byte[], byte[]> skipList = new ConcurrentSkipListMap<>(DEFAULT_COMPARATOR);
        AtomicInteger approximateSize = new AtomicInteger();
        return new MemoryTable(id, skipList, WriteAheadLog.recover(path, skipList, approximateSize), approximateSize);
    }

    public void put(byte @NotNull [] key, byte @NotNull [] value) throws IOException {
        skipList.put(key, value);

        approximateSize.getAndAdd(key.length + value.length);

        if (wal != null) {
            wal.put(key, value);
        }
    }

    public byte @Nullable [] get(byte @NotNull [] key) {
        return skipList.get(key);
    }

    public void syncWAL() throws IOException {
        if (wal != null) {
            wal.sync();
        }
    }

    public int getId() {
        return id;
    }

    public int getApproximateSize() {
        return approximateSize.get();
    }

    public boolean isEmpty() {
        return skipList.isEmpty();
    }
}
