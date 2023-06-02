package io.geekya215.lamination.iterators;

import java.io.IOException;

public interface StorageIterator {
    byte[] key();
    byte[] value();
    boolean isValid();
    void next() throws IOException;
}
