package io.geekya215.lamination;

record Options(int blockSize, int memoryTableLimit, int sstSize, boolean enableWAL) {
}