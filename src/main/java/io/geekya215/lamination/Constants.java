package io.geekya215.lamination;

public final class Constants {
    private Constants() {
    }

    public static final byte[] MAGIC = new byte[]{0x11, 0x45, 0x14, 0x19, 0x19, 0x08, 0x10, 0x69};
    public static final byte[] EMPTY_BYTES = new byte[0];

    public static final String SST_FILE_SUFFIX = ".sst";

    public static final int SIZE_OF_U16 = 2;
    public static final int SIZE_OF_U32 = 4;
    public static final int SIZE_OF_U64 = 8;

    public static final int BYTE = 1;
    public static final int KB = BYTE << 10;
    public static final int MB = KB << 10;

}