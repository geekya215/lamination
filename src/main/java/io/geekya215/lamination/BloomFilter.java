package io.geekya215.lamination;

import io.geekya215.lamination.exception.Crc32MismatchException;
import org.jetbrains.annotations.NotNull;

import java.util.BitSet;
import java.util.zip.CRC32;

import static io.geekya215.lamination.Constants.SIZE_OF_U32;
import static io.geekya215.lamination.Constants.SIZE_OF_U64;

//
// +------------------------------------------------------------------+
// |                           Bloom Filter                           |
// +--------+---------------------+----------------------+------------+
// | bitset | false_positive(u64) | num_of_elements(u32) | crc32(u32) |
// +--------+---------------------+----------------------+------------+
//
public final class BloomFilter implements Encoder {
    private static final double DEFAULT_FALSE_POSITIVE = 0.03;

    private final @NotNull BitSet bitSet;
    // m: total bits
    // n: expected insertions
    // k: number of hashes per element
    // b: m/n, bits per insertion
    // p: expected false positive probability
    //
    // 1) Optimal k = b * ln2
    // 2) p = (1 - e ^ (-kn/m))^k
    // 3) For optimal k: p = 2 ^ (-k) ~= 0.6185^b
    // 4) For optimal k: m = -nlnp / ((ln2) ^ 2)
    private final int m;
    private final int n;
    private final int k;
    private final double p;

    public BloomFilter(int n, double p) {
        int m = calculateTotalBits(n, p);
        int k = calculateNumberOfHashes(n, p);
        this.bitSet = new BitSet(m);
        this.m = m;
        this.n = n;
        this.k = k;
        this.p = p;
    }

    public BloomFilter(@NotNull BitSet bitSet, int m, int n, int k, double p) {
        this.bitSet = bitSet;
        this.m = m;
        this.n = n;
        this.k = k;
        this.p = p;
    }

    public BloomFilter(int n) {
        this(n, DEFAULT_FALSE_POSITIVE);
    }

    public static int calculateTotalBits(int n, double p) {
        return (int) (-n * Math.log(p) / Math.pow(Math.log(2.0), 2.0));
    }

    public static int calculateNumberOfHashes(int n, double p) {
        return Math.max(1, (int) Math.round((double) (calculateTotalBits(n, p) / n) * Math.log(2)));
    }

    public void mappingHashToBitset(long hash64) {
        int high = (int) hash64;
        int low = (int) (hash64 >>> 32);

        for (int i = 1; i <= k; i++) {
            int nextHash = high + i * low;
            if (nextHash < 0) {
                nextHash = ~nextHash;
            }
            bitSet.set(nextHash % m);
        }
    }

    public void add(byte @NotNull [] key) {
        long hash64 = MurmurHash2.hash64(key, key.length);
        mappingHashToBitset(hash64);
    }

    public boolean contain(byte @NotNull [] key) {
        long hash64 = MurmurHash2.hash64(key, key.length);

        int high = (int) hash64;
        int low = (int) (hash64 >>> 32);

        for (int i = 1; i <= k; i++) {
            int nextHash = high + i * low;
            if (nextHash < 0) {
                nextHash = ~nextHash;
            }
            if (!bitSet.get(nextHash % m)) {
                return false;
            }
        }

        return true;
    }

    @Override
    public byte @NotNull [] encode() {
        byte[] bitSetBuf = bitSet.toByteArray();
        int bitSetBufLength = bitSetBuf.length;
        int length = bitSetBufLength + Constants.SIZE_OF_U64 + SIZE_OF_U32 * 2;
        final byte[] buf = new byte[length];

        int cursor = 0;

        System.arraycopy(bitSetBuf, 0, buf, cursor, bitSetBufLength);
        cursor += bitSetBufLength;

        long falsePositive = Double.doubleToLongBits(p);
        buf[cursor    ] = (byte) (falsePositive >> 56);
        buf[cursor + 1] = (byte) (falsePositive >> 48);
        buf[cursor + 2] = (byte) (falsePositive >> 40);
        buf[cursor + 3] = (byte) (falsePositive >> 32);
        buf[cursor + 4] = (byte) (falsePositive >> 24);
        buf[cursor + 5] = (byte) (falsePositive >> 16);
        buf[cursor + 6] = (byte) (falsePositive >> 8);
        buf[cursor + 7] = (byte) falsePositive;
        cursor += 8;

        int numOfElements = n;
        buf[cursor    ] = (byte) (numOfElements >> 24);
        buf[cursor + 1] = (byte) (numOfElements >> 16);
        buf[cursor + 2] = (byte) (numOfElements >> 8);
        buf[cursor + 3] = (byte) numOfElements;
        cursor += 4;

        CRC32 crc32 = new CRC32();
        crc32.update(buf, 0, cursor);
        int checksum = (int) crc32.getValue();

        buf[cursor    ] = (byte) (checksum >> 24);
        buf[cursor + 1] = (byte) (checksum >> 16);
        buf[cursor + 2] = (byte) (checksum >> 8);
        buf[cursor + 3] = (byte) checksum;

        return buf;
    }

    public static @NotNull BloomFilter decode(byte @NotNull [] buf) {
        int cursor = buf.length;

        cursor -= SIZE_OF_U32;

        int actualChecksum = (buf[cursor] & 0xFF) << 24 | (buf[cursor + 1] & 0xFF) << 16 |
                (buf[cursor + 2] & 0xFF) << 8 | (buf[cursor + 3]) & 0xFF;

        CRC32 crc32 = new CRC32();
        crc32.update(buf, 0, cursor);
        int expectedChecksum = (int) crc32.getValue();

        if (actualChecksum != expectedChecksum) {
            throw new Crc32MismatchException(expectedChecksum, actualChecksum);
        }

        cursor -= SIZE_OF_U32;
        int numOfElements = (buf[cursor] & 0xFF) << 24 | (buf[cursor + 1] & 0xFF) << 16 |
                (buf[cursor + 2] & 0xFF) << 8 | (buf[cursor + 3]) & 0xFF;

        cursor -= SIZE_OF_U64;
        long falsePositive = (buf[cursor] & 0xFFL) << 56 | (buf[cursor + 1] & 0xFFL) << 48 |
                (buf[cursor + 2] & 0xFFL) << 40 | (buf[cursor + 3] & 0xFFL) << 32 |
                (buf[cursor + 4] & 0xFFL) << 24 | (buf[cursor + 5] & 0xFFL) << 16 |
                (buf[cursor + 6] & 0xFFL) << 8 | (buf[cursor + 7] & 0xFFL);
        double p = Double.longBitsToDouble(falsePositive);

        final byte[] bitsetBuf = new byte[cursor];
        System.arraycopy(buf, 0, bitsetBuf, 0, cursor);

        int m = calculateTotalBits(numOfElements, p);
        int k = calculateNumberOfHashes(numOfElements, p);

        return new BloomFilter(BitSet.valueOf(bitsetBuf), m, numOfElements, k, p);
    }
}
