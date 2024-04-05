package io.geekya215.lamination;

import org.jetbrains.annotations.NotNull;

public final class MurmurHash2 {
    private static final int S32 = 0x9747b28c;
    private static final int M32 = 0x5bd1e995;
    private static final int R32 = 24;

    private static final int S64 = 0xe17a1465;
    private static final long M64 = 0xc6a4a7935bd1e995L;
    private static final int R64 = 47;

    private MurmurHash2() {
    }

    public static int hash32(final byte @NotNull [] data, int length) {
        return hash32(data, length, S32);
    }

    public static int hash32(final byte @NotNull [] data, int length, int seed) {
        int h = seed ^ length;

        final int nblocks = length >> 2;

        for (int i = 0; i < nblocks; i++) {
            final int index = (i << 2);
            int k = (data[index    ] & 0xFF) <<  0 |
                    (data[index + 1] & 0xFF) <<  8 |
                    (data[index + 2] & 0xFF) << 16 |
                    (data[index + 3] & 0xFF) << 24;
            k *= M32;
            k ^= k >>> R32;
            k *= M32;
            h *= M32;
            h ^= k;
        }

        final int index = (nblocks << 2);
        switch (length - index) {
            case 3:
                h ^= (data[index + 2] & 0xFF) << 16;
            case 2:
                h ^= (data[index + 1] & 0xFF) << 8;
            case 1:
                h ^= (data[index    ] & 0xFF);
                h *= M32;
        }

        h ^= h >>> 13;
        h *= M32;
        h ^= h >>> 15;

        return h;
    }

    public static long hash64(final byte @NotNull [] data, int length) {
        return hash64(data, length, S64);
    }

    public static long hash64(final byte @NotNull [] data, int length, int seed) {
        long h = (seed & 0xffffffffL) ^ (length * M64);

        final int nblocks = length >> 3;

        for (int i = 0; i < nblocks; i++) {
            final int index = (i << 3);
            long k = (data[index    ] & 0xFFL) <<  0 |
                     (data[index + 1] & 0xFFL) <<  8 |
                     (data[index + 2] & 0xFFL) << 16 |
                     (data[index + 3] & 0xFFL) << 24 |
                     (data[index + 4] & 0xFFL) << 32 |
                     (data[index + 5] & 0xFFL) << 40 |
                     (data[index + 6] & 0xFFL) << 48 |
                     (data[index + 7] & 0xFFL) << 56;

            k *= M64;
            k ^= k >>> R64;
            k *= M64;

            h ^= k;
            h *= M64;
        }

        final int index = (nblocks << 3);
        switch (length - index) {
            case 7:
                h ^= ((long) data[index + 6] & 0xff) << 48;
            case 6:
                h ^= ((long) data[index + 5] & 0xff) << 40;
            case 5:
                h ^= ((long) data[index + 4] & 0xff) << 32;
            case 4:
                h ^= ((long) data[index + 3] & 0xff) << 24;
            case 3:
                h ^= ((long) data[index + 2] & 0xff) << 16;
            case 2:
                h ^= ((long) data[index + 1] & 0xff) << 8;
            case 1:
                h ^= ((long) data[index    ] & 0xff);
                h *= M64;
        }

        h ^= h >>> R64;
        h *= M64;
        h ^= h >>> R64;

        return h;
    }
}
