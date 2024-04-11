package io.geekya215.lamination;

import io.geekya215.lamination.compact.CompactStrategy;

public record Options(int blockSize, int memoryTableLimit, int sstSize, boolean enableWAL, CompactStrategy strategy) {
}