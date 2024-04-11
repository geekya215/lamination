package io.geekya215.lamination.compact;

import io.geekya215.lamination.Storage;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.List;

public final class NoCompactCompactor extends Compactor {
    public NoCompactCompactor(CompactStrategy strategy) {
        super(strategy);
    }

    @Override
    public @Nullable CompactionTask generateCompactionTask(@NotNull Storage storage) {
        return null;
    }

    @Override
    public @NotNull List<Integer> doCompact(@NotNull Storage storage, @NotNull CompactionTask task, @NotNull List<Integer> output) {
        throw new UnsupportedOperationException();
    }
}
