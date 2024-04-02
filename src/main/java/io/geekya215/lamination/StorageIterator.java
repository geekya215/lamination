package io.geekya215.lamination;

import org.jetbrains.annotations.NotNull;

public interface StorageIterator {
    byte @NotNull [] key();

    byte @NotNull [] value();

    boolean isValid();

    void next();
}
