package io.geekya215.lamination;

import io.geekya215.lamination.iterators.MergeIterator;
import io.geekya215.lamination.iterators.StorageIterator;
import io.geekya215.lamination.util.Preconditions;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

public final class LsmStorage {
    private MemTable memTable;
    private final List<MemTable> immMemTables;
    private final List<SSTable> l0SSTables;
    private final List<List<SSTable>> levels;
    private long nextSSTId;
    private final ReentrantLock rwLock;
    private final ReentrantLock flushLock;
    private final Path path;
    private final LRUCache<Long, Block> blockCache;

    public LsmStorage(Path path) {
        this.memTable = MemTable.create();
        this.immMemTables = new ArrayList<>();
        this.l0SSTables = new ArrayList<>();
        this.levels = new ArrayList<>();
        this.nextSSTId = 1;
        this.rwLock = new ReentrantLock();
        this.flushLock = new ReentrantLock();
        this.path = path;
        this.blockCache = new LRUCache<>();
    }

    public static LsmStorage open(Path path) {
        return new LsmStorage(path);
    }

    public byte[] get(byte[] key) throws IOException {
        rwLock.lock();
        try {
            // Search on the current memtable.
            ByteBuffer inMemTable = memTable.get(ByteBuffer.wrap(key));
            if (inMemTable != null) {
                if (Arrays.equals(inMemTable.array(), Constants.EMPTY_BYTES)) {
                    return null;
                } else {
                    return inMemTable.array();
                }
            }
            // Search on immutable memtables.
            for (int i = immMemTables.size() - 1; i > 0; i--) {
                ByteBuffer inImmMemTable = immMemTables.get(i).get(ByteBuffer.wrap(key));
                if (inImmMemTable != null) {
                    if (Arrays.equals(inImmMemTable.array(), Constants.EMPTY_BYTES)) {
                        return null;
                    } else {
                        return inImmMemTable.array();
                    }
                }
            }

            int cnt = 0;
            for (SSTable sst : l0SSTables) {
                cnt += sst.containKey(key) ? 1 : 0;
            }

            List<StorageIterator> iters = new ArrayList<>(cnt);
            for (SSTable sst : l0SSTables) {
                if (sst.containKey(key)) {
                    iters.add(SSTable.SSTableIterator.createAndSeekToKey(sst, key));
                }
            }
            MergeIterator iter = MergeIterator.create(iters);
            return iter.isValid() ? iter.value() : null;
        } finally {
            rwLock.unlock();
        }
    }

    public void put(byte[] key, byte[] value) throws IOException {
        rwLock.lock();
        try {
            Preconditions.checkArgument(key.length > 0, "key must not be empty");
            Preconditions.checkArgument(value.length > 0, "value must not be empty");
            memTable.put(ByteBuffer.wrap(key), ByteBuffer.wrap(value));
        } finally {
            rwLock.unlock();
        }
    }

    public void delete(byte[] key) {
        rwLock.lock();
        try {
            Preconditions.checkArgument(key.length > 0, "key must not be empty");
            memTable.put(ByteBuffer.wrap(key), ByteBuffer.wrap(Constants.EMPTY_BYTES));
        } finally {
            rwLock.unlock();
        }
    }

    public void sync() throws IOException {
        flushLock.lock();
        try {
            rwLock.lock();
            try {
                // Move mutable memtable to immutable memtables.
                MemTable flushMemTable = memTable;
                immMemTables.add(memTable);
                memTable = MemTable.create();
                long sstId = nextSSTId;

                SSTable.SSTableBuilder ssTableBuilder = new SSTable.SSTableBuilder();
                flushMemTable.flush(ssTableBuilder);
                SSTable sst = ssTableBuilder.build(sstId, path.toFile(), blockCache);

                // Add the flushed L0 table to the list.
                immMemTables.remove(immMemTables.size() - 1);
                l0SSTables.add(sst);
                nextSSTId += 1;

            } finally {
                rwLock.unlock();
            }
        } finally {
            flushLock.unlock();
        }
    }
}
