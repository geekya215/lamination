package io.geekya215.lamination.exception;

public class Crc32MismatchException extends RuntimeException {
    private static final String TEMPLATE = "Expected %d as the CRC32 checksum but the actual calculated checksum was %d";

    public Crc32MismatchException(int expected, int actual) {
        this(String.format(TEMPLATE, expected, actual));
    }

    public Crc32MismatchException(String message) {
        super(message);
    }
}
