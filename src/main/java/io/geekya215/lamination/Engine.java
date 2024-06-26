package io.geekya215.lamination;

import io.geekya215.lamination.compact.*;
import io.geekya215.lamination.iterator.*;
import io.geekya215.lamination.recover.Manifest;
import io.geekya215.lamination.recover.Track;
import io.geekya215.lamination.tuple.Tuple2;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static io.geekya215.lamination.Constants.EMPTY_BYTE_ARRAY;
import static io.geekya215.lamination.Constants.MB;
import static java.util.FormatProcessor.FMT;

public final class Engine implements Closeable {
    static final String WAL_FILE_FORMAT = "%05d.wal";
    static final String SST_FILE_FORMAT = "%05d.sst";
    static final String MANIFEST_FILE_NAME = "MANIFEST";
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
    private final @NotNull Manifest manifest;
    private final @NotNull ExecutorService flushThread;
    private final @NotNull Compactor compactor;
    private final @NotNull ExecutorService compactThread;

    public Engine(
            @NotNull Storage storage,
            @NotNull ReentrantReadWriteLock rwLock,
            @NotNull ReentrantLock lock,
            @NotNull Cache<Long, Block> blockCache,
            @NotNull Options options,
            @NotNull Path path,
            @NotNull AtomicInteger sstId,
            @NotNull Manifest manifest,
            @NotNull ExecutorService flushThread,
            @NotNull Compactor compactor,
            @NotNull ExecutorService compactThread) {
        this.storage = storage;
        this.rwLock = rwLock;
        this.readLock = rwLock.readLock();
        this.writeLock = rwLock.writeLock();
        this.lock = lock;
        this.blockCache = blockCache;
        this.options = options;
        this.path = path;
        this.sstId = sstId;
        this.manifest = manifest;
        this.flushThread = flushThread;
        this.compactor = compactor;
        this.compactThread = compactThread;
    }

    public static @NotNull Engine open(@NotNull Path path, @NotNull Options options) throws IOException {
        Cache<Long, Block> blockCache = new LRUCache<>(32 * MB);
        ScheduledExecutorService flushThread = Executors.newSingleThreadScheduledExecutor();
        ScheduledExecutorService compactThread = Executors.newSingleThreadScheduledExecutor();
        int nextSSTId = 1;

        Compactor compactor =  switch (options.strategy()) {
            case CompactStrategy.Simple simple -> new SimpleCompactor(simple);
            case CompactStrategy.NoCompact noCompact -> new NoCompactCompactor(noCompact);
            default -> throw new IllegalArgumentException("unsupported compaction strategy: " + options.strategy());
        };

        Storage storage = Storage.create(options);
        Manifest manifest;

        Path manifestPath = path.resolve(MANIFEST_FILE_NAME);
        if (Files.exists(manifestPath)) {
            Tuple2<Manifest, List<Track>> recover = Manifest.recover(manifestPath);
            manifest = recover.t1();
            List<Track> tracks = recover.t2();
            TreeSet<Integer> memoryTables = new TreeSet<>();
            for (Track track : tracks) {
                switch (track) {
                    case Track.Flush(int id) -> {
                        memoryTables.remove(id);
                        if (compactor.flushToLevel0()) {
                            storage.getLevel0SortedStringTables().add(id);
                        } else {
                            // Todo
                        }
                    }
                    case Track.Create(int id) -> {
                        nextSSTId = Math.max(nextSSTId, id);
                        memoryTables.add(id);
                    }
                    case Track.Compact(CompactionTask task, List<Integer> outputs) -> {
                        compactor.doCompact(storage, task, outputs);
                        nextSSTId = Math.max(nextSSTId, Collections.max(outputs));
                    }
                }
            }
            int sstCnt = 0;
            for (Integer sstId : storage.getLevel0SortedStringTables()) {
                SortedStringTable sst = SortedStringTable.open(sstId, blockCache, SortedStringTable.FileObject.open(getPathOfSST(path, sstId)));
                storage.getSortedStringTables().put(sstId, sst);
                sstCnt += 1;
            }
            for (Tuple2<Integer, List<Integer>> level : storage.getLevels()) {
                for (Integer sstId : level.t2()) {
                    SortedStringTable sst = SortedStringTable.open(sstId, blockCache, SortedStringTable.FileObject.open(getPathOfSST(path, sstId)));
                    storage.getSortedStringTables().put(sstId, sst);
                    sstCnt += 1;
                }
            }

            nextSSTId += 1;
            System.out.println(sstCnt + " SSTs opened");

            // recover memory table
            if (options.enableWAL()) {
                int walCnt = 0;
                for (int memoryTableId : memoryTables) {
                    MemoryTable memoryTable = MemoryTable.recoverFromWAL(memoryTableId, getPathOfWAL(path, memoryTableId));
                    if (!memoryTable.isEmpty()) {
                        storage.getImmutableMemoryTables().add(memoryTable);
                        walCnt += 1;
                    } else {
                        // Fixme
                        // empty wal id in manifest create track also remove
                        memoryTable.close();
                        Files.deleteIfExists(getPathOfWAL(path, memoryTableId));
                    }
                }
                System.out.println(walCnt + " WALs recovered");
                storage.setMemoryTable(MemoryTable.createWithWAL(nextSSTId, getPathOfWAL(path, nextSSTId)));
            } else {
                storage.setMemoryTable(MemoryTable.create(nextSSTId));
            }

            nextSSTId += 1;

        } else {
            if (options.enableWAL()) {
                storage.setMemoryTable(MemoryTable.createWithWAL(storage.getMemoryTable().getId(), getPathOfWAL(path, storage.getMemoryTable().getId())));
            }
            manifest = Manifest.create(manifestPath);
            manifest.addTrack(new Track.Create(storage.getMemoryTable().getId()));
        }

        Engine engine = new Engine(
                storage, new ReentrantReadWriteLock(), new ReentrantLock(), blockCache,
                options, path, new AtomicInteger(nextSSTId), manifest, flushThread, compactor, compactThread);

        flushThread.scheduleWithFixedDelay(() -> {
            try {
                engine.triggerFlush();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }, 10, 50, TimeUnit.MILLISECONDS);

        compactThread.scheduleWithFixedDelay(() -> {
            try {
                engine.triggerCompact();
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

        compactThread.shutdown();
        try {
            if (!compactThread.awaitTermination(100, TimeUnit.MILLISECONDS)) {
                compactThread.shutdownNow();
            }
        } catch (InterruptedException e) {
            compactThread.shutdownNow();
        }


        // Todo
        // persist in memory data

        // release resource
        readLock.lock();
        try {
            storage.getMemoryTable().close();

            for (SortedStringTable sst : storage.getSortedStringTables().values()) {
                sst.close();
            }
        } finally {
            readLock.unlock();
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
        if (key.length == 0) {
            throw new IllegalArgumentException("key must not be empty");
        }

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

            List<StorageIterator> levelIters = new ArrayList<>(storage.getLevels().size());
            for (Tuple2<Integer, List<Integer>> level : storage.getLevels()) {
                List<SortedStringTable> levelSSTs = new ArrayList<>(level.t2().size());
                for (Integer sstId : level.t2()) {
                    SortedStringTable sst = storage.getSortedStringTables().get(sstId);
                    if (Arrays.compare(sst.getFirstKey(), key) <= 0 && Arrays.compare(key, sst.getLastKey()) <= 0 && sst.getBloomFilter().contain(key)) {
                        levelSSTs.add(sst);
                    }
                }
                levelIters.add(ConcatIterator.createAndSeekToKey(levelSSTs, key));
            }

            TwoMergeIterator<MergeIterator, MergeIterator> iter = TwoMergeIterator.create(l0Iterator, MergeIterator.create(levelIters));

            if (iter.isValid() && Arrays.compare(iter.key(), key) == 0 && Arrays.compare(iter.value(), DELETE_TOMBSTONE) != 0) {
                return iter.value();
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

    boolean rangeOverlap(
            @NotNull Bound<byte[]> lower,
            @NotNull Bound<byte[]> upper,
            byte @NotNull [] firstKey,
            byte @NotNull [] lastKey) {
        switch (upper) {
            case Bound.Excluded<byte[]>(byte[] key) when Arrays.compare(key, firstKey) <= 0 -> {
                return false;
            }
            case Bound.Included<byte[]>(byte[] key) when Arrays.compare(key, firstKey) < 0 -> {
                return false;
            }
            default -> {}
        }

        switch (lower) {
            case Bound.Excluded<byte[]>(byte[] key) when Arrays.compare(key, lastKey) >= 0 -> {
                return false;
            }
            case Bound.Included<byte[]>(byte[] key) when Arrays.compare(key, lastKey) > 0 -> {
                return false;
            }
            default -> {}
        }

        return true;
    }

    public @NotNull StorageIterator scan(@NotNull Bound<byte[]> lower, @NotNull Bound<byte[]> upper) throws IOException {
        readLock.lock();
        try {
            List<MemoryTable> immutableMemoryTables = storage.getImmutableMemoryTables();
            List<StorageIterator> memoryTablesIters = new ArrayList<>(immutableMemoryTables.size() + 1);
            for (int i = immutableMemoryTables.size() - 1; i >= 0; i--) {
                MemoryTable immutableMemoryTable = immutableMemoryTables.get(i);
                memoryTablesIters.add(immutableMemoryTable.scan(lower, upper));
            }
            memoryTablesIters.add(storage.getMemoryTable().scan(lower, upper));
            StorageIterator memoryTableIter = MergeIterator.create(memoryTablesIters);

            List<StorageIterator> level0SSTIters = new ArrayList<>(storage.getLevel0SortedStringTables().size());

            for (Integer sstId : storage.getLevel0SortedStringTables()) {
                SortedStringTable sst = storage.getSortedStringTables().get(sstId);
                if (rangeOverlap(lower, upper, sst.getFirstKey(), sst.getLastKey())) {
                    StorageIterator iter = switch (lower) {
                        case Bound.Included<byte[]>(byte[] key) -> SortedStringTable.SortedStringTableIterator.createAndSeekToKey(sst, key);
                        case Bound.Excluded<byte[]>(byte[] key) -> {
                            SortedStringTable.SortedStringTableIterator tmpIter = SortedStringTable.SortedStringTableIterator.createAndSeekToKey(sst, key);
                            if (tmpIter.isValid() && Arrays.compare(tmpIter.key(), key) == 0) {
                                tmpIter.next();
                            }
                            yield tmpIter;
                        }
                        default -> SortedStringTable.SortedStringTableIterator.createAndSeekToFirst(sst);
                    };
                    level0SSTIters.add(iter);
                }
            }

            StorageIterator level0Iter = MergeIterator.create(level0SSTIters);

            List<StorageIterator> levelIters = new ArrayList<>(storage.getLevels().size());
            for (Tuple2<Integer, List<Integer>> level : storage.getLevels()) {
                List<SortedStringTable> levelSSTs = new ArrayList<>(level.t2().size());
                for (Integer sstId : level.t2()) {
                    SortedStringTable sst = storage.getSortedStringTables().get(sstId);
                    if (rangeOverlap(lower, upper, sst.getFirstKey(), sst.getLastKey())) {
                        levelSSTs.add(sst);
                    }
                }
                StorageIterator levelIter =  switch (lower) {
                    case Bound.Included<byte[]>(byte[] key) -> ConcatIterator.createAndSeekToKey(levelSSTs, key);
                    case Bound.Excluded<byte[]>(byte[] key) -> {
                        ConcatIterator tmpIter = ConcatIterator.createAndSeekToKey(levelSSTs, key);
                        if (tmpIter.isValid() && Arrays.compare(tmpIter.key(), key) == 0) {
                            tmpIter.next();
                        }
                        yield tmpIter;
                    }
                    default -> ConcatIterator.createAndSeekToFirst(levelSSTs);
                };
                levelIters.add(levelIter);
            }

            TwoMergeIterator<StorageIterator, StorageIterator> memoryToLevel0Iter = TwoMergeIterator.create(memoryTableIter, level0Iter);
            // Todo
            // cast to StorageIterator or use raw generic ?
            TwoMergeIterator<StorageIterator, StorageIterator> iter = TwoMergeIterator.create(memoryToLevel0Iter, MergeIterator.create(levelIters));

            return LsmIterator.create(iter, upper);
        } finally {
            readLock.unlock();
        }
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

        manifest.addTrack(new Track.Create(memoryTableId));
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

            if (options.enableWAL()) {
                oldestImmutableMemoryTable.close();
                Files.deleteIfExists(getPathOfWAL(path, sstId));
            }

            manifest.addTrack(new Track.Flush(sstId));
        } finally {
            lock.unlock();
        }
    }

    private @NotNull List<SortedStringTable> buildCompactedSSTFromIterator(@NotNull StorageIterator iter, boolean compactToBottomLevel) throws IOException {
        SortedStringTable.SortedStringTableBuilder builder = null;
        final List<SortedStringTable> ssts = new ArrayList<>();

        while (iter.isValid()) {
            if (builder == null) {
                builder = new SortedStringTable.SortedStringTableBuilder(options.blockSize());
            }

            if (compactToBottomLevel) {
                // only put not deleted value
                if (iter.value().length != 0) {
                    builder.put(iter.key(), iter.value());
                }
            } else {
                builder.put(iter.key(), iter.value());
            }

            iter.next();

            if (builder.estimateSize() >= options.sstSize()) {
                int sstId = getNextSSTId();
                SortedStringTable sst = builder.build(sstId, blockCache, getPathOfSST(path, sstId));
                ssts.add(sst);
                builder = null;
            }
        }

        if (builder != null) {
            int sstId = getNextSSTId();
            SortedStringTable sst = builder.build(sstId, blockCache, getPathOfSST(path, sstId));
            ssts.add(sst);
        }

        return ssts;
    }

    public @NotNull List<SortedStringTable> compact(@NotNull CompactionTask task) throws IOException {
        readLock.lock();
        try {
            final Map<Integer, SortedStringTable> ssts = storage.getSortedStringTables();
            switch (task) {
                case CompactionTask.SimpleTask simple -> {
                    if (simple.upperLevel() == 0) {
                        final List<StorageIterator> upperIters = new ArrayList<>(simple.upperLevelSSTIds().size());
                        for (Integer upperSSTId : simple.upperLevelSSTIds()) {
                            upperIters.add(SortedStringTable.SortedStringTableIterator.createAndSeekToFirst(ssts.get(upperSSTId)));
                        }
                        final StorageIterator upperIter = MergeIterator.create(upperIters);

                        final List<SortedStringTable> lowerSSTs = new ArrayList<>(simple.lowerLevelSSTIds().size());
                        for (Integer lowerSSTId : simple.lowerLevelSSTIds()) {
                            lowerSSTs.add(ssts.get(lowerSSTId));
                        }

                        final ConcatIterator lowerIter = ConcatIterator.createAndSeekToFirst(lowerSSTs);
                        return buildCompactedSSTFromIterator(TwoMergeIterator.create(upperIter, lowerIter), simple.isLowerLevelBottomLevel());
                    } else {
                        final List<SortedStringTable> upperSSTs = new ArrayList<>(simple.upperLevelSSTIds().size());
                        for (Integer upperSSTId : simple.upperLevelSSTIds()) {
                            upperSSTs.add(ssts.get(upperSSTId));
                        }

                        final ConcatIterator upperIter = ConcatIterator.createAndSeekToFirst(upperSSTs);

                        final List<SortedStringTable> lowerSSTs = new ArrayList<>(simple.lowerLevelSSTIds().size());
                        for (Integer lowerSSTId : simple.lowerLevelSSTIds()) {
                            lowerSSTs.add(ssts.get(lowerSSTId));
                        }

                        final ConcatIterator lowerIter = ConcatIterator.createAndSeekToFirst(lowerSSTs);
                        return buildCompactedSSTFromIterator(TwoMergeIterator.create(upperIter, lowerIter), simple.isLowerLevelBottomLevel());
                    }
                }
                default -> throw new UnsupportedOperationException();
            }
        } finally {
            readLock.unlock();
        }
    }

    public void forceFullCompaction() {
        readLock.lock();
        try {

        } finally {
            readLock.unlock();
        }

        List<SortedStringTable> newSSTs;

        lock.lock();
        try {
            writeLock.lock();
            try {

            } finally {
                writeLock.unlock();
            }
        } finally {
            lock.unlock();
        }
    }

    void triggerCompact() throws IOException {
        readLock.lock();
        try {
            final CompactionTask task = compactor.generateCompactionTask(storage);
            if (task == null) {
                return ;
            } else {
                final List<SortedStringTable> compactedSSTs = compact(task);
                final List<Integer> outputs = compactedSSTs.stream().map(SortedStringTable::getId).toList();
                List<SortedStringTable> removedSSTs;
                lock.lock();
                try {
                    // Todo
                    // should we use double check here?
                    readLock.lock();
                    final List<Integer> newSSTIds = new ArrayList<>();
                    try {
                        for (SortedStringTable compactedSST : compactedSSTs) {
                            newSSTIds.add(compactedSST.getId());
                            storage.getSortedStringTables().put(compactedSST.getId(), compactedSST);
                        }

                        final List<Integer> filesToRemove = compactor.doCompact(storage, task, outputs);
                        removedSSTs = new ArrayList<>(filesToRemove.size());
                        for (Integer file : filesToRemove) {
                            SortedStringTable removedSST = storage.getSortedStringTables().remove(file);
                            removedSSTs.add(removedSST);
                        }
                    } finally {
                        readLock.unlock();
                    }
                    manifest.addTrack(new Track.Compact(task, newSSTIds));
                } finally {
                    lock.unlock();
                }

                // delete removed sst
                for (SortedStringTable removedSST : removedSSTs) {
                    removedSST.close();
                    Files.deleteIfExists(getPathOfSST(path, removedSST.getId()));
                }
            }
        } finally {
            readLock.unlock();
        }
    }

    public void dump() {
        System.out.println(FMT."MEM -> \{storage.getMemoryTable().getId()}");
        System.out.println(FMT."IMM -> \{storage.getImmutableMemoryTables().stream().map(MemoryTable::getId).toList()}");
        System.out.println(FMT."L 0 -> \{storage.getLevel0SortedStringTables()}");
        for (Tuple2<Integer, List<Integer>> level : storage.getLevels()) {
            System.out.println(FMT."L%2d\{level.t1()} -> \{level.t2()}");
        }
    }
}
