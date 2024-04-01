package io.geekya215.lamination;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public final class Engine {
    static final String WAL_FILE_FORMAT = "%05d.wal";
    static final String SST_FILE_FORMAT = "%05d.sst";
    static final byte[] DELETE_TOMBSTONE = new byte[0];
    private final @NotNull Storage storage;
    private final @NotNull ReentrantReadWriteLock rwLock;
    private final @NotNull ReentrantReadWriteLock.ReadLock readLock;
    private final @NotNull ReentrantReadWriteLock.WriteLock writeLock;
    private final @NotNull ReentrantLock lock;
    private final @NotNull Options options;
    private final @NotNull Path path;
    private final @NotNull AtomicInteger sstId;

    public Engine(
            @NotNull Storage storage,
            @NotNull ReentrantReadWriteLock rwLock,
            @NotNull ReentrantLock lock,
            @NotNull Options options,
            @NotNull Path path,
            @NotNull AtomicInteger sstId) {
        this.storage = storage;
        this.rwLock = rwLock;
        this.readLock = rwLock.readLock();
        this.writeLock = rwLock.writeLock();
        this.lock = lock;
        this.options = options;
        this.path = path;
        this.sstId = sstId;
    }

    public static @NotNull Engine open(@NotNull Path path, @NotNull Options options) {
        return new Engine(Storage.create(options), new ReentrantReadWriteLock(), new ReentrantLock(), options, path, new AtomicInteger());
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
        readLock.lock();
        int approximateSize;
        try {
            MemoryTable memoryTable = storage.getMemoryTable();
            memoryTable.put(key, value);
            approximateSize = memoryTable.getApproximateSize();
        } finally {
            readLock.unlock();
        }
        tryFreeze(approximateSize);
    }

    public byte @Nullable [] get(byte @NotNull [] key) {
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

            // search in l0 or other level sst in the future

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
}
