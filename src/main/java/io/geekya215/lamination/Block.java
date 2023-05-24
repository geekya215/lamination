package io.geekya215.lamination;

import io.geekya215.lamination.util.FileUtil;
import io.geekya215.lamination.util.IOUtil;
import io.geekya215.lamination.util.Preconditions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static io.geekya215.lamination.Constants.*;

//
//  -----------------------------------------------------------------------
// |                               Block                                   |
// |-----------------------------------------------------------------------|
// |  crc32  | num of elements |   offsets(2B/per)   |       entries       |
// |---------+-----------------+---------------------+---------------------|
// |    4B   |        2B       |  2B  |  2B  |  ...  |  ?B  |  ?B  |  ...  |
//  -----------------------------------------------------------------------
//
public final class Block implements Measurable {
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
        IOUtil.writeU32AsU16(bytes, index, numOfElements);
        index += 2;

        // offsets
        for (short o : offsets) {
            IOUtil.writeU16(bytes, index, o);
            index += 2;
        }

        // data
        IOUtil.writeBytes(bytes, index, data);

        // crc32
        FileUtil.writeCRC32(bytes);

        return bytes;
    }

    public static Block decode(byte[] bytes) {
        int blockSize = bytes.length;

        FileUtil.checkCRC32(bytes);

        //  -------------------------
        // | crc32 | num of elements |
        // |-------+-----------------|
        // |   4B  |       2B        |
        //  -------------------------
        // ^       ^                 ^
        // 0       4                 6
        int index = 4;
        int numOfElements = IOUtil.readU16AsU32(bytes, index);
        index += 2;

        short[] offsets = new short[numOfElements];
        byte[] data = new byte[blockSize - SIZE_OF_U32 - SIZE_OF_U16 - (numOfElements << 1)];

        for (int i = 0; i < numOfElements; i++) {
            offsets[i] = IOUtil.readU16(bytes, index);
            index += 2;
        }
        IOUtil.readBytes(data, index, bytes);
        return new Block(offsets, data);
    }

    public byte[] firstKey() {
        int keySize = IOUtil.readU16AsU32(data, 0);
        byte[] key = new byte[keySize];
        IOUtil.readBytes(key, 4, data);
        return key;
    }

    @Override
    public int estimateSize() {
        return SIZE_OF_U32 + SIZE_OF_U16 + 2 * offsets.length + data.length;
    }

    public static final class BlockBuilder {
        private final List<Byte> offsetByteList;
        private final List<Byte> dataByteList;
        private final int blockSize;
        private int currentSize;

        public BlockBuilder() {
            this(Options.DEFAULT_BLOCK_SIZE);
        }

        public BlockBuilder(int blockSize) {
            Preconditions.checkArgument(blockSize > 0, "block size must greater than 0 byte");
            this.offsetByteList = new ArrayList<>();
            this.dataByteList = new ArrayList<>();
            this.blockSize = blockSize;
            this.currentSize = 0;
        }

        //
        //  -------------------------------------------
        // |                   entry                   |
        // |-------------------------------------------|
        // | key_len | value_len |    key   |   value  |
        // |---------+-----------+----------+----------|
        // |    2B   |     2B    | keylen B | varlen B |
        //  ------------------------------------------
        //
        public boolean put(byte[] key, byte[] value) {
            int keySize = key.length;
            int valueSize = value.length;
            int entrySize = 2 * SIZE_OF_U16 + keySize + valueSize;

            // Todo
            // for reducing call hierarchy we should check key and value at engine
            Preconditions.checkArgument(keySize > 0, "key must not be empty");
            Preconditions.checkArgument(
                entrySize + SIZE_OF_U32 + 2 * SIZE_OF_U16 <= blockSize,
                "entry size too large");

            if (currentSize + entrySize + SIZE_OF_U32 + 2 * SIZE_OF_U16 > blockSize) {
                return false;
            }

            offsetByteList.add((byte) (currentSize >> 8));
            offsetByteList.add((byte) currentSize);

            dataByteList.add((byte) (keySize >> 8));
            dataByteList.add((byte) keySize);
            dataByteList.add((byte) (valueSize >> 8));
            dataByteList.add((byte) valueSize);

            for (byte k : key) {
                dataByteList.add(k);
            }

            for (byte v : value) {
                dataByteList.add(v);
            }

            currentSize += entrySize;

            return true;
        }

        // avoid create new builder
        public void reset() {
            offsetByteList.clear();
            dataByteList.clear();
            currentSize = 0;
        }

        public Block build() {
            Preconditions.checkState(!offsetByteList.isEmpty(), "block should not be empty");

            short[] offset = new short[offsetByteList.size() >>> 1];
            byte[] data = new byte[dataByteList.size()];

            int index = 0;
            for (int i = 0; i < offsetByteList.size() >>> 1; i++) {
                offset[i] = (short) (((offsetByteList.get(index) & 0xff) << 8) | (offsetByteList.get(index + 1) & 0xff));
                index += 2;
            }

            for (int i = 0; i < dataByteList.size(); i++) {
                data[i] = dataByteList.get(i);
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
            return key.length != 0;
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
            int keySize = IOUtil.readU16AsU32(data, offset);
            offset += 2;
            int valueSize = IOUtil.readU16AsU32(data, offset);
            offset += 2;

            byte[] keyBytes = new byte[keySize];
            byte[] valueBytes = new byte[valueSize];
            IOUtil.readBytes(keyBytes, offset, data);
            IOUtil.readBytes(valueBytes, offset + keySize, data);

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
