package io.geekya215.lamination.compact;

import io.geekya215.lamination.Storage;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.List;

public sealed abstract class Compactor permits SimpleCompactor, LeveledCompactor, TieredCompactor, NoCompactCompactor {
    protected CompactStrategy strategy;

    public Compactor(CompactStrategy strategy) {
        this.strategy = strategy;
    }

    protected boolean flushToLevel0() {
        return switch (this) {
            case SimpleCompactor _, LeveledCompactor _, NoCompactCompactor _ -> true;
            default -> false;
        };
    }

    public abstract @Nullable CompactionTask generateCompactionTask(@NotNull Storage storage);

    public abstract @NotNull List<Integer> doCompact(@NotNull Storage storage, @NotNull CompactionTask task, @NotNull List<Integer> output);
}
