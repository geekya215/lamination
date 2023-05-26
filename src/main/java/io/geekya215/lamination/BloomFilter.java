package io.geekya215.lamination;

import io.geekya215.lamination.util.FileUtil;
import io.geekya215.lamination.util.ByteUtil;

import java.util.BitSet;

//
//  ----------------------------------------------------------------
// |                           bloom filter                         |
// |----------------------------------------------------------------|
// | crc32 | false positive | num of element | bitset size | bitset |
// |-------+----------------+----------------+-------------+--------|
// |   4B  |       8B       |       4B       |      4B     |   ?B   |
//  ----------------------------------------------------------------
//
public final class BloomFilter {
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

    public static int calculateTotalBits(int n, double p) {
        return (int) (-n * Math.log(p) / Math.pow(Math.log(2.0), 2.0));
    }

    public static int calculateNumberOfHashes(int n, double p) {
        return Math.max(1, (int) Math.round((double) (calculateTotalBits(n, p) / n) * Math.log(2)));
    }

    public BloomFilter(int n) {
        this(n, DEFAULT_FALSE_POSITIVE);
    }

    public BloomFilter(int n, double p) {
        int m = calculateTotalBits(n, p);
        int k = calculateNumberOfHashes(n, p);

        this.bitSet = new BitSet(m);
        this.m = m;
        this.n = n;
        this.k = k;
        this.p = p;
    }

    public BloomFilter(BitSet bitSet, int m, int n, int k, double p) {
        this.bitSet = bitSet;
        this.m = m;
        this.n = n;
        this.k = k;
        this.p = p;
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
        byte[] bitsetBytes = bitSet.toByteArray();
        int bitsetSize = bitsetBytes.length;

        int bytesSize = Constants.SIZE_OF_U32 * 3 + Constants.SIZE_OF_U64 + bitsetSize;
        byte[] bytes = new byte[bytesSize];

        int index = 4;
        long pToLong = Double.doubleToLongBits(p);

        ByteUtil.writeU64(bytes, index, pToLong);
        index += 8;

        ByteUtil.writeU32(bytes, index, n);
        index += 4;

        ByteUtil.writeU32(bytes, index, bitsetSize);
        index += 4;

        ByteUtil.writeAllBytes(bytes, index, bitsetBytes);

        FileUtil.writeCRC32(bytes);

        return bytes;
    }

    public static BloomFilter decode(byte[] bytes) {
        FileUtil.checkCRC32(bytes);

        int index = 4;

        long longToP = ByteUtil.readU64(bytes, index);
        double p = Double.longBitsToDouble(longToP);
        index += 8;

        int n = ByteUtil.readU32(bytes, index);
        index += 4;

        int bitsetSize = ByteUtil.readU32(bytes, index);
        index += 4;

        byte[] bitsetBytes = new byte[bitsetSize];
        ByteUtil.readAllBytes(bitsetBytes, index, bytes);
        BitSet bitSet = BitSet.valueOf(bitsetBytes);

        int m = calculateTotalBits(n, p);
        int k = calculateNumberOfHashes(n, p);

        return new BloomFilter(bitSet, m, n, k, p);
    }
}
