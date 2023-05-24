package io.geekya215.lamination.util;

import io.geekya215.lamination.Constants;
import io.geekya215.lamination.exception.Crc32MismatchException;

import java.io.File;
import java.io.IOException;
import java.util.zip.CRC32;

public final class FileUtil {
    private static final CRC32 crc32 = new CRC32();

    private FileUtil() {
    }

    public static void checkCRC32(byte[] bytes) {
        int size = bytes.length;

        crc32.update(bytes, 4, size - 4);
        int expectedCheckSum = (int) crc32.getValue();
        crc32.reset();

        int actualCheckSum = IOUtil.readU32(bytes, 0);

        if (expectedCheckSum != actualCheckSum) {
            throw new Crc32MismatchException(expectedCheckSum, actualCheckSum);
        }
    }

    public static void writeCRC32(byte[] bytes) {
        int size = bytes.length;

        crc32.update(bytes, 4, size - 4);
        int checkSum = (int) crc32.getValue();
        crc32.reset();

        IOUtil.writeU32(bytes, 0, checkSum);
    }

    public static File makeFile(File baseDir, String filename) throws IOException {
        Preconditions.checkState(baseDir.exists() && baseDir.isDirectory());
        File file = new File(baseDir, filename);
        file.createNewFile();
        return file;
    }

    public static String makeFileName(long id, String suffix) {
        return String.format("%06d%s", id, suffix);
    }

    public static File makeSSTableFile(File baseDir, long id) throws IOException {
        return makeFile(baseDir, makeFileName(id, Constants.SST_FILE_SUFFIX));
    }
}
