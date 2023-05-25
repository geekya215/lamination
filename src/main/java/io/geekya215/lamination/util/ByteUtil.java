package io.geekya215.lamination.util;

// NOTICE
// all methods do not check array index
public final class ByteUtil {
    public static void writeU16(byte[] dest, int index, short u16) {
        dest[index    ] = (byte) (u16 >> 8);
        dest[index + 1] = (byte) (u16     );
    }

    public static void writeU32AsU16(byte[] dest, int index, int u32) {
        dest[index    ] = (byte) (u32 >> 8);
        dest[index + 1] = (byte) (u32     );
    }

    public static void writeU32(byte[] dest, int index, int u32) {
        dest[index    ] = (byte) (u32 >> 24);
        dest[index + 1] = (byte) (u32 >> 16);
        dest[index + 2] = (byte) (u32 >>  8);
        dest[index + 3] = (byte) (u32      );
    }

    public static void writeU64(byte[] dest, int index, long u64) {
        dest[index    ] = (byte) (u64 >> 56);
        dest[index + 1] = (byte) (u64 >> 48);
        dest[index + 2] = (byte) (u64 >> 40);
        dest[index + 3] = (byte) (u64 >> 32);
        dest[index + 4] = (byte) (u64 >> 24);
        dest[index + 5] = (byte) (u64 >> 16);
        dest[index + 6] = (byte) (u64 >>  8);
        dest[index + 7] = (byte) (u64      );
    }

    public static void writeNBytes(byte[] dest, int index, byte[] src, int len) {
        System.arraycopy(src, 0, dest, index, len);
    }

    public static void writeAllBytes(byte[] dest, int index, byte[] src) {
        writeNBytes(dest, index, src, src.length);
    }

    public static short readU16(byte[] src, int index) {
        return (short) ((src[index    ] & 0xff) << 8 |
                        (src[index + 1] & 0xff));
    }

    public static int readU16AsU32(byte[] src, int index) {
        return (src[index    ] & 0xff) << 8 |
               (src[index + 1] & 0xff);
    }

    public static int readU32(byte[] src, int index) {
        return (src[index    ] & 0xff) << 24 |
               (src[index + 1] & 0xff) << 16 |
               (src[index + 2] & 0xff) <<  8 |
               (src[index + 3] & 0xff);
    }

    public static long readU64(byte[] src, int index) {
        return (src[index    ] & 0xffL) << 56 |
               (src[index + 1] & 0xffL) << 48 |
               (src[index + 2] & 0xffL) << 40 |
               (src[index + 3] & 0xffL) << 32 |
               (src[index + 4] & 0xffL) << 24 |
               (src[index + 5] & 0xffL) << 16 |
               (src[index + 6] & 0xffL) <<  8 |
               (src[index + 7] & 0xffL);
    }

    public static void readNBytes(byte[] dest, int index, byte[] src, int len) {
        System.arraycopy(src, index, dest, 0, len);
    }

    public static void readAllBytes(byte[] dest, int index, byte[] src) {
        readNBytes(dest, index, src, dest.length);
    }
}

