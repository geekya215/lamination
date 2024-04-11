package io.geekya215.lamination.compact;

import io.geekya215.lamination.Storage;
import io.geekya215.lamination.tuple.Tuple2;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

public final class SimpleCompactor extends Compactor {
    public SimpleCompactor(CompactStrategy strategy) {
        super(strategy);
    }

    @Override
    public @Nullable CompactionTask generateCompactionTask(@NotNull Storage storage) {
        final CompactStrategy.Simple option = (CompactStrategy.Simple) strategy;
        final List<Tuple2<Integer, List<Integer>>> levels = storage.getLevels();
        final List<Integer> levelSizes = new ArrayList<>();

        levelSizes.add(storage.getLevel0SortedStringTables().size());

        for (Tuple2<Integer, List<Integer>> level : levels) {
            levelSizes.add(level.t2().size());
        }

        for (int i = 0; i < option.maxLevels(); i++) {
            if (i == 0 && storage.getLevel0SortedStringTables().size() < option.maxNumOfLevel0Files()) {
                continue;
            }
            int lowerLevel = i + 1;
            double sizeRatio = (double) levelSizes.get(lowerLevel) / (double) levelSizes.get(i);
            if (sizeRatio < option.sizeRatioPercent() / 100.0) {
                int upperLevel = i;
                // Todo
                // use immutable list?
                final List<Integer> upperLevelSSTIds =
                        upperLevel == 0
                                ? new ArrayList<>(storage.getLevel0SortedStringTables())
                                : new ArrayList<>(levels.get(upperLevel - 1).t2());
                final List<Integer> lowerLevelSSTIds = new ArrayList<>(levels.get(lowerLevel - 1).t2());
                return new CompactionTask.SimpleTask(upperLevel, upperLevelSSTIds, lowerLevel,
                        lowerLevelSSTIds, lowerLevel == option.maxLevels());
            }
        }

        return null;
    }


    @Override
    public @NotNull List<Integer> doCompact(@NotNull Storage storage, @NotNull CompactionTask task, @NotNull List<Integer> output) {
        CompactionTask.SimpleTask simpleTask = (CompactionTask.SimpleTask) task;
        final List<Integer> fileToRemove = new ArrayList<>();
        final List<Tuple2<Integer, List<Integer>>> levels = storage.getLevels();

        if (simpleTask.upperLevel() == 0) {
            // level 0 compaction

            // add upper level files to remove list
            fileToRemove.addAll(simpleTask.upperLevelSSTIds());

            // remove level0 sst which in upper level
            final HashSet<Integer> level0SSTCompacted = new HashSet<>(simpleTask.upperLevelSSTIds());
            final List<Integer> newLevel0SST = storage.getLevel0SortedStringTables().stream().filter(x -> !level0SSTCompacted.remove(x)).toList();

            // set new level0 sst
            storage.getLevel0SortedStringTables().clear();
            storage.getLevel0SortedStringTables().addAll(newLevel0SST);
        } else {
            // level N compaction

            // add upper level to remove list and clear
            fileToRemove.addAll(levels.get(simpleTask.upperLevel() - 1).t2());
            levels.get(simpleTask.upperLevel() - 1).t2().clear();
        }

        // add lower level to remove list and set to output
        fileToRemove.addAll(levels.get(simpleTask.lowerLevel() - 1).t2());
        levels.get(simpleTask.lowerLevel() - 1).t2().clear();
        levels.get(simpleTask.lowerLevel() - 1).t2().addAll(output);

        return fileToRemove;
    }
}
