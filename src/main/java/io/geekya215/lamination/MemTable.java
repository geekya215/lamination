package io.geekya215.lamination;

import io.geekya215.lamination.iterators.StorageIterator;

import java.io.IOException;
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

    public MemTableIterator scan(ByteBuffer lower, ByteBuffer upper) {
        Iterator<Map.Entry<ByteBuffer, ByteBuffer>> iter = skipList
            .entrySet()
            .stream()
            .filter(entry -> entry.getKey().compareTo(lower) >= 0 && entry.getKey().compareTo(upper) <= 0)
            .iterator();
        return new MemTableIterator(iter);
    }

    public void flush(SSTable.SSTableBuilder ssTableBuilder) {
        skipList.forEach((k, v) -> ssTableBuilder.put(k.array(), v.array()));
    }

    public long size() {
        return currentSize.get();
    }

    public static final class MemTableIterator implements StorageIterator {
        private final Iterator<Map.Entry<ByteBuffer, ByteBuffer>> iter;
        private Map.Entry<ByteBuffer, ByteBuffer> current;

        public MemTableIterator(Iterator<Map.Entry<ByteBuffer, ByteBuffer>> iter) {
            this.iter = iter;
            this.current = this.iter.hasNext() ? this.iter.next() : null;
        }

        public static MemTableIterator create(MemTable memTable) {
            return new MemTableIterator(memTable.skipList.entrySet().iterator());
        }

        @Override
        public byte[] key() {
            return current.getKey().array();
        }

        @Override
        public byte[] value() {
            return current.getValue().array();
        }

        @Override
        public boolean isValid() {
            return current != null;
        }

        @Override
        public void next() throws IOException {
            current = iter.hasNext() ? iter.next() : null;
        }
    }
}
