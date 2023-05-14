package io.geekya215.lamination;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

public final class MemTable {
    private final ConcurrentSkipListMap<ByteBuffer, ByteBuffer> skipList;
    private final AtomicLong currentSize;

    public MemTable(ConcurrentSkipListMap<ByteBuffer, ByteBuffer> skipList) {
        this.skipList = skipList;
        this.currentSize = new AtomicLong(0L);
    }

    public static MemTable create() {
       return new MemTable(new ConcurrentSkipListMap<>());
    }

    public ByteBuffer get(ByteBuffer key) {
        return skipList.get(key);
    }

    public void put(ByteBuffer key, ByteBuffer value) {
        int increment = key.limit() + value.limit();
        currentSize.addAndGet(increment);
        skipList.put(key, value);
    }

    public Iterator<Map.Entry<ByteBuffer, ByteBuffer>> scan(ByteBuffer lower, ByteBuffer upper) {
        return skipList
            .entrySet()
            .stream()
            .filter(entry -> entry.getKey().compareTo(lower) >= 0 && entry.getKey().compareTo(upper) <= 0)
            .iterator();
    }

    public void flush(SSTable.SSTableBuilder ssTableBuilder) {
        skipList.forEach((k, v) -> ssTableBuilder.put(k.array(), v.array()));
    }

    public long size() {
        return currentSize.get();
    }
}
