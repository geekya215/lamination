package io.geekya215.lamination.compact;

import org.jetbrains.annotations.NotNull;

import java.util.List;

public sealed interface CompactionTask
        permits CompactionTask.SimpleTask, CompactionTask.LeveledTask, CompactionTask.TieredTask, CompactionTask.FullTask {
    record SimpleTask(
            int upperLevel,
            @NotNull List<Integer> upperLevelSSTIds,
            int lowerLevel,
            @NotNull List<Integer> lowerLevelSSTIds,
            boolean isLowerLevelBottomLevel) implements CompactionTask {
    }

    record LeveledTask() implements CompactionTask {
    }

    record TieredTask() implements CompactionTask {
    }

    record FullTask() implements CompactionTask {
    }
}
