package io.geekya215.lamination;

import io.geekya215.lamination.Bound.Excluded;
import io.geekya215.lamination.Bound.Included;
import io.geekya215.lamination.Bound.Unbounded;
import io.geekya215.lamination.tuple.Tuple2;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
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

    // NOTICE
    // if upper < lower this method will throw IllegalArgumentException
    public @NotNull MemoryTableIterator scan(@NotNull Bound<byte[]> lower, @NotNull Bound<byte[]> upper) {
        Tuple2<Bound<byte[]>, Bound<byte[]>> range = Tuple2.of(lower, upper);

        ConcurrentNavigableMap<byte[], byte[]> result = switch (range) {
            case Tuple2(Included<byte[]> l, Included<byte[]> u) -> skipList.subMap(l.value(), true, u.value(), true);
            case Tuple2(Included<byte[]> l, Excluded<byte[]> u) -> skipList.subMap(l.value(), true, u.value(), false);
            case Tuple2(Included<byte[]> l, Unbounded<byte[]> _) -> skipList.tailMap(l.value(), true);
            case Tuple2(Excluded<byte[]> l, Included<byte[]> u) -> skipList.subMap(l.value(), false, u.value(), true);
            case Tuple2(Excluded<byte[]> l, Excluded<byte[]> u) -> skipList.subMap(l.value(), false, u.value(), false);
            case Tuple2(Excluded<byte[]> l, Unbounded<byte[]> _) -> skipList.tailMap(l.value(), false);
            case Tuple2(Unbounded<byte[]> _, Included<byte[]> u) -> skipList.headMap(u.value(), true);
            case Tuple2(Unbounded<byte[]> _, Excluded<byte[]> u) -> skipList.headMap(u.value(), false);
            case Tuple2(Unbounded<byte[]> _, Unbounded<byte[]> _) -> skipList;
        };

        return new MemoryTableIterator(result);
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

    public static final class MemoryTableIterator implements StorageIterator {
        static final byte @NotNull [] EMPTY_BYTES = new byte[0];
        static final @NotNull Map.Entry<byte[], byte[]> INITIAL_ITEM = Map.entry(EMPTY_BYTES, EMPTY_BYTES);

        private final @NotNull ConcurrentNavigableMap<byte[], byte[]> skipList;
        private final @NotNull Iterator<Map.Entry<byte[], byte[]>> iter;
        private @NotNull Map.Entry<byte[], byte[]> current;

        public MemoryTableIterator(@NotNull ConcurrentNavigableMap<byte[], byte[]> skipList) {
            this.skipList = skipList;
            this.iter = skipList.entrySet().iterator();
            this.current = INITIAL_ITEM;
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
            current = iter.hasNext() ? iter.next() : INITIAL_ITEM;
        }
    }
}
