package io.geekya215.lamination;

import io.geekya215.lamination.exception.Crc32MismatchException;

import java.util.BitSet;
import java.util.zip.CRC32;

//
//  ----------------------------------------------------------------------------------
// |                                   Bloom filter                                   |
// |---------+--------------------+--------------------+-----------------+------------|
// |crc32(4B)| false positive(8B) | num of element(4B) | bitset size(4B) | bitset(?B) |
//  ----------------------------------------------------------------------------------
//
public final class BloomFilter {
    // Todo
    // share same crc instance with Block?
    private static final CRC32 crc32 = new CRC32();
    private static final double DEFAULT_FALSE_POSITIVE = 0.03;
    private final BitSet bitSet;
    /*
     * m: total bits
     * n: expected insertions
     * k: number of hashes per element
     * b: m/n, bits per insertion
     * p: expected false positive probability
     *
     * 1) Optimal k = b * ln2
     * 2) p = (1 - e ^ (-kn/m))^k
     * 3) For optimal k: p = 2 ^ (-k) ~= 0.6185^b
     * 4) For optimal k: m = -nlnp / ((ln2) ^ 2)
     */
    private final int m;
    private final int n;
    private final int k;
    private final double p;

    public BloomFilter(BitSet bitSet, int m, int n, int k, double p) {
        this.bitSet = bitSet;
        this.m = m;
        this.n = n;
        this.k = k;
        this.p = p;
    }

    public BloomFilter(int n, double p) {
        int m = (int) (-n * Math.log(p) / Math.pow(Math.log(2.0), 2.0));
        int k = Math.max(1, (int) Math.round((double) (m / n) * Math.log(2)));

        this.bitSet = new BitSet(m);
        this.m = m;
        this.n = n;
        this.k = k;
        this.p = p;
    }

    public BloomFilter(int n) {
        this(n, DEFAULT_FALSE_POSITIVE);
    }

    public void add(byte[] data) {
        long hash64 = MurmurHash2.hash64(data, data.length);

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

    public boolean contain(byte[] data) {
        long hash64 = MurmurHash2.hash64(data, data.length);

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

    public byte[] encode() {
        byte[] bitSetByteArray = bitSet.toByteArray();
        int bitsetSize = bitSetByteArray.length;

        int bytesSize = Constants.SIZE_OF_U32 * 3 + Constants.SIZE_OF_U64 + bitsetSize;
        byte[] bytes = new byte[bytesSize];

        int index = 4;
        long pToLong = Double.doubleToLongBits(p);

        bytes[index + 0] = (byte) (pToLong >> 56);
        bytes[index + 1] = (byte) (pToLong >> 48);
        bytes[index + 2] = (byte) (pToLong >> 40);
        bytes[index + 3] = (byte) (pToLong >> 32);
        bytes[index + 4] = (byte) (pToLong >> 24);
        bytes[index + 5] = (byte) (pToLong >> 16);
        bytes[index + 6] = (byte) (pToLong >> 8);
        bytes[index + 7] = (byte) (pToLong >> 0);
        index += 8;

        bytes[index + 0] = (byte) (n >> 24);
        bytes[index + 1] = (byte) (n >> 16);
        bytes[index + 2] = (byte) (n >> 8);
        bytes[index + 3] = (byte) (n >> 0);
        index += 4;

        bytes[index + 0] = (byte) (bitsetSize >> 24);
        bytes[index + 1] = (byte) (bitsetSize >> 16);
        bytes[index + 2] = (byte) (bitsetSize >> 8);
        bytes[index + 3] = (byte) (bitsetSize >> 0);
        index += 4;

        System.arraycopy(bitSetByteArray, 0, bytes, index, bitsetSize);

        crc32.update(bytes, 4, bytesSize - 4);
        int checkSum = (int) crc32.getValue();
        crc32.reset();

        bytes[0] = (byte) (checkSum >> 24);
        bytes[1] = (byte) (checkSum >> 16);
        bytes[2] = (byte) (checkSum >> 8);
        bytes[3] = (byte) (checkSum >> 0);

        return bytes;
    }

    public static BloomFilter decode(byte[] bytes) {
        int blockSize = bytes.length;

        crc32.update(bytes, 4, blockSize - 4);
        int expectedCheckSum = (int) crc32.getValue();
        crc32.reset();

        int actualCheckSum
            = (bytes[0] & 0xff) << 24
            | (bytes[1] & 0xff) << 16
            | (bytes[2] & 0xff) << 8
            | (bytes[3] & 0xff) << 0;

        if (expectedCheckSum != actualCheckSum) {
            throw new Crc32MismatchException(expectedCheckSum, actualCheckSum);
        }

        int index = 4;
        long longToP
            = ((bytes[index + 0] & 0xffL) << 56)
            | ((bytes[index + 1] & 0xffL) << 48)
            | ((bytes[index + 2] & 0xffL) << 40)
            | ((bytes[index + 3] & 0xffL) << 32)
            | ((bytes[index + 4] & 0xffL) << 24)
            | ((bytes[index + 5] & 0xffL) << 16)
            | ((bytes[index + 6] & 0xffL) << 8)
            | ((bytes[index + 7] & 0xffL) << 0);
        double p = Double.longBitsToDouble(longToP);
        index += 8;

        int n
            = ((bytes[index + 0] & 0xff) << 24)
            | ((bytes[index + 1] & 0xff) << 16)
            | ((bytes[index + 2] & 0xff) << 8)
            | ((bytes[index + 3] & 0xff) << 0);
        index += 4;

        int bitsetSize
            = ((bytes[index + 0] & 0xff) << 24)
            | ((bytes[index + 1] & 0xff) << 16)
            | ((bytes[index + 2] & 0xff) << 8)
            | ((bytes[index + 3] & 0xff) << 0);
        index += 4;

        byte[] bitsetBytes = new byte[bitsetSize];
        System.arraycopy(bytes, index, bitsetBytes, 0, bitsetSize);
        BitSet bitSet = BitSet.valueOf(bitsetBytes);

        int m = (int) (-n * Math.log(p) / Math.pow(Math.log(2.0), 2.0));
        int k = Math.max(1, (int) Math.round((double) (m / n) * Math.log(2)));

        return new BloomFilter(bitSet, m, n, k, p);
    }
}
