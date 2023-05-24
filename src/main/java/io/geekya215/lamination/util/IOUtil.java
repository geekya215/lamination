package io.geekya215.lamination.util;

// NOTICE
// all methods do not check array index
public final class IOUtil {
    public static void writeU16(byte[] data, int index, short u16) {
        data[index] = (byte) (u16 >> 8);
        data[index + 1] = (byte) u16;
    }

    public static void writeU32AsU16(byte[] data, int index, int u32) {
        data[index] = (byte) (u32 >> 8);
        data[index + 1] = (byte) u32;
    }

    public static void writeU32(byte[] data, int index, int u32) {
        data[index] = (byte) (u32 >> 24);
        data[index + 1] = (byte) (u32 >> 16);
        data[index + 2] = (byte) (u32 >> 8);
        data[index + 3] = (byte) u32;
    }

    public static void writeU64(byte[] data, int index, long u64) {
        data[index] = (byte) (u64 >> 56);
        data[index + 1] = (byte) (u64 >> 48);
        data[index + 2] = (byte) (u64 >> 40);
        data[index + 3] = (byte) (u64 >> 32);
        data[index + 4] = (byte) (u64 >> 24);
        data[index + 5] = (byte) (u64 >> 16);
        data[index + 6] = (byte) (u64 >> 8);
        data[index + 7] = (byte) u64;
    }

    public static void writeBytes(byte[] dest, int index, byte[] src) {
        System.arraycopy(src, 0, dest, index, src.length);
    }

    public static short readU16(byte[] data, int index) {
        return (short) (((data[index] & 0xff) << 8) | (data[index + 1] & 0xff));
    }

    public static int readU16AsU32(byte[] data, int index) {
        return ((data[index] & 0xff) << 8) | (data[index + 1] & 0xff);
    }

    public static int readU32(byte[] data, int index) {
        return (data[index] & 0xff) << 24
            | (data[index + 1] & 0xff) << 16
            | (data[index + 2] & 0xff) << 8
            | (data[index + 3] & 0xff);
    }

    public static long readU64(byte[] data, int index) {
        return (data[index] & 0xffL) << 56
            | (data[index + 1] & 0xffL) << 48
            | (data[index + 2] & 0xffL) << 40
            | (data[index + 3] & 0xffL) << 32
            | (data[index + 4] & 0xffL) << 24
            | (data[index + 5] & 0xffL) << 16
            | (data[index + 6] & 0xffL) << 8
            | (data[index + 7] & 0xffL);
    }

    public static void readBytes(byte[] dest, int index, byte[] src) {
        System.arraycopy(src, index, dest, 0, dest.length);
    }
}
