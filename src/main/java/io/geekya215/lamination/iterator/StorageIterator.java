package io.geekya215.lamination.iterator;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.Map;

import static io.geekya215.lamination.Constants.EMPTY_BYTE_ARRAY;

public interface StorageIterator {
    Map.Entry<byte[], byte[]> EMPTY_ENTRY = Map.entry(EMPTY_BYTE_ARRAY, EMPTY_BYTE_ARRAY);

    byte @NotNull [] key();

    byte @NotNull [] value();

    boolean isValid();

    void next() throws IOException;
}
