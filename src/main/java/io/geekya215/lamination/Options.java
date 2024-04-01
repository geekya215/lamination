package io.geekya215.lamination;

public record Options(int blockSize, int memoryTableLimit, int sstSize, boolean enableWAL) {
}