package io.geekya215.lamination;

import io.geekya215.lamination.exception.Crc32MismatchException;
import io.geekya215.lamination.util.Preconditions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.zip.CRC32;

import static io.geekya215.lamination.Constants.*;

//
//  --------------------------------------------------------------------
// |                            Block Size                              |
// |---------+-------------------+--------------------+-----------------|
// |         |                   |   offsets(2B/per)  |     entries     |
// |crc32(4B)|num_of_elements(2B)|offset|offset|offset|entry|entry|entry|
//  --------------------------------------------------------------------
//
public final class Block implements Measurable {
    private static final CRC32 crc32 = new CRC32();

    private final short[] offsets;
    private final byte[] data;

    public Block(short[] offsets, byte[] data) {
        this.offsets = offsets;
        this.data = data;
    }

    public short[] getOffsets() {
        return offsets;
    }

    public byte[] getData() {
        return data;
    }

    public byte[] encode() {
        int blockSize = estimateSize();
        byte[] bytes = new byte[blockSize];
        int index = 4;

        // num of elements
        int numOfElements = offsets.length;
        bytes[index++] = (byte) (numOfElements >> 8);
        bytes[index++] = (byte) numOfElements;

        // offsets
        for (short o : offsets) {
            bytes[index++] = (byte) (o >> 8);
            bytes[index++] = (byte) o;
        }

        // data
        for (byte d : data) {
            bytes[index++] = d;
        }

        // crc32
        crc32.update(bytes, 4, blockSize - 4);
        int checkSum = (int) crc32.getValue();
        crc32.reset();

        bytes[0] = (byte) (checkSum >> 24);
        bytes[1] = (byte) (checkSum >> 16);
        bytes[2] = (byte) (checkSum >> 8);
        bytes[3] = (byte) checkSum;

        return bytes;
    }

    public static Block decode(byte[] bytes) {
        int blockSize = bytes.length;

        crc32.update(bytes, 4, blockSize - 4);
        int expectedCheckSum = (int) crc32.getValue();
        crc32.reset();

        //  -------------------------
        // | crc32 | num_of_elements |
        // |  4(B) |       2(B)      |
        // ^-------^-----------------^
        // 0       4                 6
        int actualCheckSum
            = (bytes[0] & 0xff) << 24
            | (bytes[1] & 0xff) << 16
            | (bytes[2] & 0xff) << 8
            | (bytes[3] & 0xff);

        if (expectedCheckSum != actualCheckSum) {
            throw new Crc32MismatchException(expectedCheckSum, actualCheckSum);
        }

        int numOfElements = (bytes[4] & 0xff) << 8 | (bytes[5] & 0xff);
        short[] offsets = new short[numOfElements];
        byte[] data = new byte[blockSize - SIZE_OF_U32 - SIZE_OF_U16 - (numOfElements << 1)];

        int index = 6;
        for (int i = 0; i < numOfElements; i++) {
            offsets[i] = (short) (((bytes[index] & 0xff) << 8) | (bytes[index + 1] & 0xff));
            index += 2;
        }
        System.arraycopy(bytes, index, data, 0, data.length);

        return new Block(offsets, data);
    }

    public byte[] firstKey() {
        int keySize = ((data[0] & 0xff) << 8) | (data[1] & 0xff);
        byte[] key = new byte[keySize];
        System.arraycopy(data, 4, key, 0, keySize);
        return key;
    }

    @Override
    public int estimateSize() {
        return SIZE_OF_U32 + SIZE_OF_U16 + 2 * offsets.length + data.length;
    }

    public static final class BlockBuilder {
        private final List<Byte> offsetsBytes;
        private final List<Byte> dataBytes;
        private final int blockSize;
        private int currentSize;

        public BlockBuilder() {
            this(Options.DEFAULT_BLOCK_SIZE);
        }

        public BlockBuilder(int blockSize) {
            Preconditions.checkArgument(blockSize > 0, "block size must greater than 0 byte");
            this.offsetsBytes = new ArrayList<>();
            this.dataBytes = new ArrayList<>();
            this.blockSize = blockSize;
            this.currentSize = 0;
        }

        //
        //  ---------------------------------------------------------------
        // |                             entry                             |
        // |--------------+----------------+--------------+----------------|
        // | key_len (2B) | value_len (2B) | key (keylen) | value (varlen) |
        //  ---------------------------------------------------------------
        //
        public boolean put(byte[] key, byte[] value) {
            int keySize = key.length;
            int valueSize = value.length;
            int entrySize = 2 * SIZE_OF_U16 + keySize + valueSize;

            Preconditions.checkArgument(keySize > 0, "key must not be empty");
            Preconditions.checkArgument(
                entrySize + SIZE_OF_U32 + 2 * SIZE_OF_U16 <= blockSize,
                "entry size too large");

            if (currentSize + entrySize + SIZE_OF_U32 + 2 * SIZE_OF_U16 > blockSize) {
                return false;
            }

            offsetsBytes.add((byte) (currentSize >> 8));
            offsetsBytes.add((byte) currentSize);

            dataBytes.add((byte) (keySize >> 8));
            dataBytes.add((byte) keySize);
            dataBytes.add((byte) (valueSize >> 8));
            dataBytes.add((byte) valueSize);

            for (byte k : key) {
                dataBytes.add(k);
            }

            for (byte v : value) {
                dataBytes.add(v);
            }

            currentSize += entrySize;

            return true;
        }

        // avoid create new builder
        public void reset() {
            offsetsBytes.clear();
            dataBytes.clear();
            currentSize = 0;
        }

        public Block build() {
            Preconditions.checkState(!offsetsBytes.isEmpty(), "block should not be empty");

            short[] offset = new short[offsetsBytes.size() >>> 1];
            byte[] data = new byte[dataBytes.size()];

            int index = 0;
            for (int i = 0; i < offsetsBytes.size() >>> 1; i++) {
                offset[i] = (short) (((offsetsBytes.get(index) & 0xff) << 8) | (offsetsBytes.get(index + 1) & 0xff));
                index += 2;
            }

            for (int i = 0; i < dataBytes.size(); i++) {
                data[i] = dataBytes.get(i);
            }

            return new Block(offset, data);
        }
    }

    public static final class BlockIterator {
        private final Block block;
        private byte[] key;
        private byte[] value;
        private int index;

        public static BlockIterator createAndSeekToFirst(Block block) {
            BlockIterator blockIterator = new BlockIterator(block);
            blockIterator.seekTo(0);
            return blockIterator;
        }

        public static BlockIterator createAndSeekToKey(Block block, byte[] key) {
            BlockIterator blockIterator = new BlockIterator(block);
            blockIterator.seekToKey(key);
            return blockIterator;
        }

        public BlockIterator(Block block) {
            this.block = block;
            this.key = EMPTY_BYTES;
            this.value = EMPTY_BYTES;
            this.index = -1;
        }

        public byte[] getKey() {
            return key;
        }

        public byte[] getValue() {
            return value;
        }

        public void next() {
            index += 1;
            seekTo(index);
        }

        public boolean isValid() {
            return !(key.length == 0);
        }

        public void seekToFirst() {
            seekTo(0);
        }

        public void seekTo(int index) {
            if (index >= block.getOffsets().length) {
                key = EMPTY_BYTES;
                value = EMPTY_BYTES;
            } else {
                int offset = block.getOffsets()[index];
                seekToOffset(offset);
                this.index = index;
            }
        }

        void seekToOffset(int offset) {
            byte[] data = block.getData();
            int keySize = ((data[offset] & 0xff) << 8) | (data[offset + 1] & 0xff);
            int valueSize = ((data[offset + 2] & 0xff) << 8) | (data[offset + 3] & 0xff);
            offset += 4;

            byte[] keyBytes = new byte[keySize];
            byte[] valueBytes = new byte[valueSize];
            System.arraycopy(data, offset, keyBytes, 0, keySize);
            System.arraycopy(data, offset + keySize, valueBytes, 0, valueSize);

            key = keyBytes;
            value = valueBytes;
        }

        // Seek to the first key that >= `key`.
        public void seekToKey(byte[] key) {
            int low = 0;
            int high = block.getOffsets().length;
            while (low < high) {
                int mid = low + (high - low) / 2;
                seekTo(mid);
                assert isValid();
                int cmp = Arrays.compare(this.key, key);
                if (cmp < 0) {
                    low = mid + 1;
                } else if (cmp == 0) {
                    return;
                } else {
                    high = mid;
                }
            }

            seekTo(low);
        }
    }
}
