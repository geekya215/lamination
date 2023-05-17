package io.geekya215.lamination;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

public final class Engine {
    private final AtomicLong seq;
    private final File baseDir;
    private MemTable memTable;
    private MemTable immutableMemTable;
    private final int memtableSize;
    private final ReentrantLock lock = new ReentrantLock();

    public Engine(File baseDir) {
        this.seq = new AtomicLong(0L);
        this.memTable = MemTable.create();
        this.immutableMemTable = null;
        this.memtableSize = Options.DEFAULT_MEM_TABLE_SIZE;
        this.baseDir = baseDir;
    }

    public Engine(File baseDir, int memtableSize) {
        this.seq = new AtomicLong(0L);
        this.memTable = MemTable.create();
        this.immutableMemTable = null;
        this.memtableSize = memtableSize;
        this.baseDir = baseDir;
    }

    public void put(byte[] key, byte[] value) throws IOException {
        try {
            lock.lock();
            if (memTable.size() > memtableSize) {
                if (immutableMemTable != null) {
                    SSTable.SSTableBuilder ssTableBuilder = new SSTable.SSTableBuilder();
                    immutableMemTable.flush(ssTableBuilder);
                    ssTableBuilder.build(seq.incrementAndGet(), baseDir);
                }
                immutableMemTable = memTable;
                memTable = MemTable.create();
            } else {
                memTable.put(ByteBuffer.wrap(key), ByteBuffer.wrap(value));
            }
        } finally {
            lock.unlock();
        }
    }

    public ByteBuffer get(byte[] key) throws IOException {
        try {
            lock.lock();
            // step 1: find key in memtable
            // step 2: find key in immemtable
            // step 3: find in sst (can be optimized with bloom filter)
            ByteBuffer wrapKey = ByteBuffer.wrap(key);

            // find in memtable
            ByteBuffer inMemTable = memTable.get(wrapKey);
            if (inMemTable != null) {
                return inMemTable;
            } else {
                if (immutableMemTable == null) {
                    return null;
                } else {
                    // find in immemtable
                    ByteBuffer inImmutableMemTable = immutableMemTable.get(wrapKey);
                    if (inImmutableMemTable != null) {
                        return inImmutableMemTable;
                    } else {
                        // find in sst
                        for (int i = 1; i <= seq.get(); ++i) {
                            try (SSTable sst = SSTable.open(i, baseDir)) {
                                SSTable.SSTableIterator iterator = SSTable.SSTableIterator.createAndSeekToKey(sst, key);
                                if (Arrays.equals(iterator.getKey(), key)) {
                                    return ByteBuffer.wrap(iterator.getValue());
                                }
                            }
                        }
                        return null;
                    }
                }
            }
        } finally {
            lock.unlock();
        }
    }
}
