package io.geekya215.lamination;

import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.nio.file.Path;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.ReentrantLock;

public final class WriteAheadLog {
    private final @NotNull File file;
    private final @NotNull ReentrantLock lock;

    public WriteAheadLog(@NotNull File file) {
        this.file = file;
        this.lock = new ReentrantLock();
    }

    public static WriteAheadLog create(@NotNull Path path) {
        return new WriteAheadLog(path.toFile());
    }

    public static WriteAheadLog recover(@NotNull Path path, @NotNull ConcurrentSkipListMap<byte[], byte[]> skipList) {
        throw new UnsupportedOperationException();
    }

    public void put(byte @NotNull [] key, byte @NotNull [] value) {
        throw new UnsupportedOperationException();
    }

    public void sync() {
        throw new UnsupportedOperationException();
    }
}
