package io.geekya215.lamination.util;

import io.geekya215.lamination.Constants;

import java.io.File;
import java.io.IOException;

public final class FileUtil {
    private FileUtil() {
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
