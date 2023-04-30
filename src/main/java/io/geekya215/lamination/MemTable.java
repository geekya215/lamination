package io.geekya215.lamination;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

public final class MemTable {
    private final ConcurrentSkipListMap<ByteBuffer, ByteBuffer> skipList;

    public MemTable(ConcurrentSkipListMap<ByteBuffer, ByteBuffer> skipList) {
        this.skipList = skipList;
    }

    public static MemTable create() {
        return new MemTable(new ConcurrentSkipListMap<>());
    }

    public ByteBuffer get(ByteBuffer key) {
        return skipList.get(key);
    }

    public void put(ByteBuffer key, ByteBuffer value) {
        skipList.put(key, value);
    }

    public Iterator<Map.Entry<ByteBuffer, ByteBuffer>> scan(ByteBuffer lower, ByteBuffer upper) {
        return skipList
            .entrySet()
            .stream()
            .filter(entry -> entry.getKey().compareTo(lower) >= 0 && entry.getKey().compareTo(upper) <= 0)
            .iterator();
    }

    public void flush(SSTable.SSTableBuilder sstIterator) {
        skipList.forEach((k, v) -> sstIterator.put(k.array(), v.array()));
    }
}
