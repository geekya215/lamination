package io.geekya215.lamination;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;

public final class MemoryTable {
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

    public static MemoryTable create(int id) {
        return new MemoryTable(id, new ConcurrentSkipListMap<>(Arrays::compare), null, new AtomicInteger());
    }

    public static MemoryTable createWithWAL(int id, @NotNull Path path) throws FileNotFoundException {
        return new MemoryTable(id, new ConcurrentSkipListMap<>(Arrays::compare), WriteAheadLog.create(path), new AtomicInteger());
    }

    public static MemoryTable recoverFromWAL(int id, @NotNull Path path) throws IOException {
        ConcurrentSkipListMap<byte[], byte[]> skipList = new ConcurrentSkipListMap<>(Arrays::compare);
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

    public byte @NotNull [] get(byte @NotNull [] key) {
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
