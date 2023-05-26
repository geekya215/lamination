package io.geekya215.lamination;

import java.io.IOException;

public interface StorageIterator {
    byte[] key();
    byte[] value();
    boolean isValid();
    void next() throws IOException;
}
