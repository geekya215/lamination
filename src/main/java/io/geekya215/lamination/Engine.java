package io.geekya215.lamination;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static io.geekya215.lamination.Constants.EMPTY_BYTE_ARRAY;
import static io.geekya215.lamination.Constants.MB;

public final class Engine implements Closeable {
    static final String WAL_FILE_FORMAT = "%05d.wal";
    static final String SST_FILE_FORMAT = "%05d.sst";
    static final byte[] DELETE_TOMBSTONE = EMPTY_BYTE_ARRAY;
    private final @NotNull Storage storage;
    private final @NotNull ReentrantReadWriteLock rwLock;
    private final @NotNull ReentrantReadWriteLock.ReadLock readLock;
    private final @NotNull ReentrantReadWriteLock.WriteLock writeLock;
    private final @NotNull ReentrantLock lock;
    private final @NotNull Cache<Long, Block> blockCache;
    private final @NotNull Options options;
    private final @NotNull Path path;
    private final @NotNull AtomicInteger sstId;
    private final @NotNull ExecutorService flushThread;

    public Engine(
            @NotNull Storage storage,
            @NotNull ReentrantReadWriteLock rwLock,
            @NotNull ReentrantLock lock,
            @NotNull Cache<Long, Block> blockCache,
            @NotNull Options options,
            @NotNull Path path,
            @NotNull AtomicInteger sstId,
            @NotNull ExecutorService flushThread) {
        this.storage = storage;
        this.rwLock = rwLock;
        this.readLock = rwLock.readLock();
        this.writeLock = rwLock.writeLock();
        this.lock = lock;
        this.blockCache = blockCache;
        this.options = options;
        this.path = path;
        this.sstId = sstId;
        this.flushThread = flushThread;
    }

    public static @NotNull Engine open(@NotNull Path path, @NotNull Options options) {
        Cache<Long, Block> blockCache = new LRUCache<>(32 * MB);
        ScheduledExecutorService flushThread = Executors.newSingleThreadScheduledExecutor();
        Engine engine = new Engine(Storage.create(options), new ReentrantReadWriteLock(), new ReentrantLock(), blockCache, options, path, new AtomicInteger(), flushThread);
        flushThread.scheduleWithFixedDelay(() -> {
            try {
                engine.triggerFlush();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }, 10, 50, TimeUnit.MILLISECONDS);
        return engine;
    }

    @Override
    public void close() throws IOException {
        flushThread.shutdown();
        try {
            if (!flushThread.awaitTermination(100, TimeUnit.MILLISECONDS)) {
                flushThread.shutdownNow();
            }
        } catch (InterruptedException e) {
            flushThread.shutdownNow();
        }
    }

    public static @NotNull Path getPathOfWAL(@NotNull Path path, int id) {
        return path.resolve(WAL_FILE_FORMAT.formatted(id));
    }

    public static @NotNull Path getPathOfSST(@NotNull Path path, int id) {
        return path.resolve(SST_FILE_FORMAT.formatted(id));
    }

    public @NotNull Storage getStorage() {
        return storage;
    }

    public int getNextSSTId() {
        return sstId.addAndGet(1);
    }

    public void put(byte @NotNull [] key, byte @NotNull [] value) throws IOException {
        int approximateSize;
        readLock.lock();
        try {
            MemoryTable memoryTable = storage.getMemoryTable();
            memoryTable.put(key, value);
            approximateSize = memoryTable.getApproximateSize();
        } finally {
            readLock.unlock();
        }
        tryFreeze(approximateSize);
    }

    public byte @Nullable [] get(byte @NotNull [] key) throws IOException {
        readLock.lock();
        try {
            // find in memory table
            byte[] resInMemoryTable = storage.getMemoryTable().get(key);
            if (resInMemoryTable != null) {
                if (resInMemoryTable != DELETE_TOMBSTONE) {
                    return resInMemoryTable;
                } else {
                    return null;
                }
            }

            // find in immutable memory table
            // imm_memtable1 -> imm_memtable2 -> imm_memtable3 -> ...
            //    oldest                              least
            List<MemoryTable> immutableMemoryTables = storage.getImmutableMemoryTables();
            for (int i = immutableMemoryTables.size() - 1; i >= 0; i--) {
                MemoryTable immMemoryTable = immutableMemoryTables.get(i);
                byte[] resInImmutableTables = immMemoryTable.get(key);

                if (resInImmutableTables != null) {
                    if (resInImmutableTables != DELETE_TOMBSTONE) {
                        return resInImmutableTables;
                    } else {
                        return null;
                    }
                }
            }

            Map<Integer, SortedStringTable> ssts = storage.getSortedStringTables();
            // search in l0 or other level sst in the future
            List<StorageIterator> level0SSTIterator = new ArrayList<>(storage.getLevel0SortedStringTables().size());

            for (Integer level0SSTId : storage.getLevel0SortedStringTables()) {
                SortedStringTable sst = ssts.get(level0SSTId);
                if (Arrays.compare(sst.getFirstKey(), key) <= 0 && Arrays.compare(key, sst.getLastKey()) <= 0 && sst.getBloomFilter().contain(key)) {
                    level0SSTIterator.add(SortedStringTable.SortedStringTableIterator.createAndSeekToKey(sst, key));
                }
            }

            MergeIterator l0Iterator = MergeIterator.create(level0SSTIterator);
            if (l0Iterator.isValid()) {
                return l0Iterator.value();
            }

            // not find
            return null;
        } finally {
            readLock.unlock();
        }
    }

    public void delete(byte @NotNull [] key) throws IOException {
        put(key, DELETE_TOMBSTONE);
    }

    void tryFreeze(int estimateSize) throws IOException {
        if (estimateSize >= options.sstSize()) {
            lock.lock();
            try {
                ReentrantReadWriteLock.ReadLock readLock = rwLock.readLock();
                readLock.lock();
                if (storage.getMemoryTable().getApproximateSize() >= options.sstSize()) {
                    readLock.unlock();
                    forceFreezeMemoryTable();
                }
            } finally {
                lock.unlock();
            }
        }
    }

    public void forceFreezeMemoryTable() throws IOException {
        int memoryTableId = getNextSSTId();
        MemoryTable newMemoryTable =
                options.enableWAL()
                        ? MemoryTable.createWithWAL(memoryTableId, getPathOfWAL(path, memoryTableId))
                        : MemoryTable.create(memoryTableId);

        MemoryTable oldMemoryTable = storage.getMemoryTable();

        writeLock.lock();
        try {
            storage.setMemoryTable(newMemoryTable);
            storage.getImmutableMemoryTables().add(oldMemoryTable);
        } finally {
            writeLock.unlock();
        }
        oldMemoryTable.syncWAL();
    }

    void triggerFlush() throws IOException {
        boolean shouldFlush;

        readLock.lock();
        try {
            shouldFlush = storage.getImmutableMemoryTables().size() >= options.memoryTableLimit();
        } finally {
            readLock.unlock();
        }

        if (shouldFlush) {
            forceFlushImmutableMemoryTable();
        }
    }

    public void forceFlushImmutableMemoryTable() throws IOException {
        lock.lock();
        try {
            MemoryTable oldestImmutableMemoryTable;
            readLock.lock();
            try {
                oldestImmutableMemoryTable = storage.getImmutableMemoryTables().getFirst();
            } finally {
                readLock.unlock();
            }

            SortedStringTable.SortedStringTableBuilder builder = new SortedStringTable.SortedStringTableBuilder(options.blockSize());
            oldestImmutableMemoryTable.flush(builder);

            int sstId = oldestImmutableMemoryTable.getId();
            SortedStringTable table = builder.build(sstId, blockCache, getPathOfSST(path, sstId));

            // remove oldest immutable memory table from list
            writeLock.lock();
            try {
                MemoryTable memoryTable = storage.getImmutableMemoryTables().removeFirst();
                storage.getLevel0SortedStringTables().add(memoryTable.getId());
                storage.getSortedStringTables().put(sstId, table);
            } finally {
                writeLock.unlock();
            }
        } finally {
            lock.unlock();
        }
    }
}
