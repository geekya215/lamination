package io.geekya215.lamination.compact;

public sealed interface CompactStrategy
        permits CompactStrategy.NoCompact, CompactStrategy.Simple, CompactStrategy.Leveled, CompactStrategy.Tiered {
    record NoCompact() implements CompactStrategy {
    }

    record Simple(int sizeRatioPercent, int maxNumOfLevel0Files, int maxLevels) implements CompactStrategy {
    }

    record Leveled() implements CompactStrategy {
    }

    record Tiered() implements CompactStrategy {
    }
}
