package io.geekya215.lamination;

import io.geekya215.lamination.exception.Crc32MismatchException;
import org.jetbrains.annotations.NotNull;

import java.io.*;
import java.nio.file.Path;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.CRC32;

public final class WriteAheadLog implements Closeable {
    private final @NotNull DataOutputStream dos;
    private final @NotNull ReentrantLock lock;

    public WriteAheadLog(@NotNull File file, boolean append) throws FileNotFoundException {
        this.dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(file, append)));
        this.lock = new ReentrantLock();
    }

    public static @NotNull WriteAheadLog create(@NotNull Path path) throws FileNotFoundException {
        return new WriteAheadLog(path.toFile(), false);
    }

    public static @NotNull WriteAheadLog recover(
            @NotNull Path path,
            @NotNull ConcurrentSkipListMap<byte[], byte[]> skipList,
            @NotNull AtomicInteger approximateSize
    ) throws IOException {
        File file = path.toFile();
        try (FileInputStream fis = new FileInputStream(file);
             DataInputStream dis = new DataInputStream(fis)
        ) {
            CRC32 crc32 = new CRC32();
            int currentSize = 0;
            while (dis.available() > 0) {
                crc32.reset();

                int keyLen = dis.readUnsignedShort();
                crc32.update(keyLen);

                byte[] key = dis.readNBytes(keyLen);
                crc32.update(key);

                int valueLen = dis.readUnsignedShort();
                crc32.update(valueLen);

                byte[] value = dis.readNBytes(valueLen);
                crc32.update(value);

                int actualChecksum = dis.readInt();
                int expectedChecksum = (int) crc32.getValue();
                if (actualChecksum != expectedChecksum) {
                    throw new Crc32MismatchException(expectedChecksum, actualChecksum);
                }
                skipList.put(key, value);
                currentSize += keyLen + valueLen;
            }
            approximateSize.getAndAdd(currentSize);
            return new WriteAheadLog(file, true);
        }
    }

    public void put(byte @NotNull [] key, byte @NotNull [] value) throws IOException {
        lock.lock();
        try {
            CRC32 crc32 = new CRC32();

            dos.writeShort(key.length);
            crc32.update(key.length);

            dos.write(key);
            crc32.update(key);

            dos.writeShort(value.length);
            crc32.update(value.length);

            dos.write(value);
            crc32.update(value);

            dos.writeInt((int) crc32.getValue());
        } finally {
            lock.unlock();
        }
    }

    public void sync() throws IOException {
        lock.lock();
        try {
            dos.flush();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() throws IOException {
        dos.close();
    }
}
