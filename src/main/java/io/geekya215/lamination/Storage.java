package io.geekya215.lamination;

import io.geekya215.lamination.compact.CompactStrategy;
import io.geekya215.lamination.tuple.Tuple2;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class Storage {
    private final @NotNull List<MemoryTable> immutableMemoryTables;
    private final @NotNull List<Integer> level0SortedStringTables;
    private final @NotNull List<Tuple2<Integer, List<Integer>>> levels;
    private final @NotNull Map<Integer, SortedStringTable> sortedStringTables;
    private @NotNull MemoryTable memoryTable;

    public Storage(
            @NotNull MemoryTable memoryTable,
            @NotNull List<MemoryTable> immutableMemoryTable,
            @NotNull List<Integer> level0SST,
            @NotNull List<Tuple2<Integer, List<Integer>>> levels,
            @NotNull Map<Integer, SortedStringTable> sortedStringTables) {
        this.memoryTable = memoryTable;
        this.immutableMemoryTables = immutableMemoryTable;
        this.level0SortedStringTables = level0SST;
        this.levels = levels;
        this.sortedStringTables = sortedStringTables;
    }

    public static @NotNull Storage create(@NotNull Options options) {
        List<Tuple2<Integer, List<Integer>>> levels = switch (options.strategy()) {
            case CompactStrategy.Simple(_, _, int maxLevel) -> {
                List<Tuple2<Integer, List<Integer>>> res = new ArrayList<>(maxLevel);
                for (int level = 1; level <= maxLevel; level++) {
                    res.add(Tuple2.of(level, new ArrayList<>()));
                }
                yield res;
            }
            default -> new ArrayList<>();
        };
        return new Storage(MemoryTable.create(0), new ArrayList<>(), new ArrayList<>(), levels, new HashMap<>());
    }

    public @NotNull MemoryTable getMemoryTable() {
        return memoryTable;
    }

    public void setMemoryTable(@NotNull MemoryTable memoryTable) {
        this.memoryTable = memoryTable;
    }

    public @NotNull List<MemoryTable> getImmutableMemoryTables() {
        return immutableMemoryTables;
    }

    public @NotNull List<Integer> getLevel0SortedStringTables() {
        return level0SortedStringTables;
    }

    public @NotNull List<Tuple2<Integer, List<Integer>>> getLevels() {
        return levels;
    }

    public @NotNull Map<Integer, SortedStringTable> getSortedStringTables() {
        return sortedStringTables;
    }
}