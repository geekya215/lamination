package io.geekya215.lamination;

public final class Constants {
    public static final byte[] EMPTY_BYTE_ARRAY = new byte[0];
    public static final byte[] MAGIC = new byte[]{0x11, 0x45, 0x14, 0x19, 0x19, 0x08, 0x10, 0x10};
    public static final int BYTE = 1;
    public static final int KB = BYTE << 10;
    public static final int MB = KB << 10;
    public static final int GB = MB << 10;
    public static final int SIZE_OF_U8 = 1;
    public static final int SIZE_OF_U16 = SIZE_OF_U8 << 1;
    public static final int SIZE_OF_U32 = SIZE_OF_U16 << 1;
    public static final int SIZE_OF_U64 = SIZE_OF_U32 << 1;

    private Constants() {
    }
}
