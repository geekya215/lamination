package io.geekya215.lamination;

import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static io.geekya215.lamination.Constants.EMPTY_BYTE_ARRAY;
import static io.geekya215.lamination.Constants.SIZE_OF_U16;

//
// +-------------------------------------------------------------------------------------------------------------+
// |                                           Block                                                             |
// +--------------------------------------+----------------------------------------------+-----------------------+
// |             Data Section             |              Offset Section                  |         Extra         |
// +----------+----------+-----+----------+----------------+-----------+-----+-----------+-----------------------+
// | entry #1 | entry #2 | ... | entry #N | offset(u16) #1 | offset #2 | ... | offset #N |  num_of_elements(u16) |
// +----------+----------+-----+----------+----------------+-----------+-----+-----------+-----------------------+
//
public record Block(byte @NotNull [] data, short @NotNull [] offsets) implements Measurable, Encoder {

    public static @NotNull Block decode(byte @NotNull [] buf) {
        int cursor = buf.length;

        // read number of elements
        cursor -= SIZE_OF_U16;
        int numOfElements = buf[cursor] << 8 | buf[cursor + 1] & 0xFF;

        // read offset section
        final short[] offsets = new short[numOfElements];
        cursor -= numOfElements * SIZE_OF_U16;
        for (int i = 0; i < numOfElements; i++) {
            int idx = cursor + i * SIZE_OF_U16;
            offsets[i] = (short) (buf[idx] << 8 | buf[idx + 1] & 0xFF);
        }

        // read data section
        final byte[] data = new byte[cursor];
        System.arraycopy(buf, 0, data, 0, cursor);

        return new Block(data, offsets);
    }

    @Override
    public byte @NotNull [] encode() {
        int dataLength = data.length;
        int numOfElements = offsets.length;

        final byte[] buf = new byte[dataLength + (numOfElements * SIZE_OF_U16) + SIZE_OF_U16];

        // write data section
        System.arraycopy(data, 0, buf, 0, dataLength);

        // write offset section
        int cursor = dataLength;
        for (short offset : offsets) {
            buf[cursor] = (byte) (offset >> 8);
            buf[cursor + 1] = (byte) offset;
            cursor += 2;
        }

        // write number of elements
        buf[cursor] = (byte) (numOfElements >> 8);
        buf[cursor + 1] = (byte) numOfElements;

        return buf;
    }

    public byte @NotNull [] getFirstKey() {
        int keyLength = data[0] << 8 | data[1] & 0xFF;
        final byte[] buf = new byte[keyLength];
        System.arraycopy(data, 2, buf, 0, keyLength);
        return buf;
    }

    // Notice
    // estimate size not equal encode byte buf length
    @Override
    public int estimateSize() {
        return data.length + offsets.length * SIZE_OF_U16;
    }

    public static final class BlockBuilder {
        private final @NotNull List<Byte> dataByteList;
        // use list of byte for offset because JDK cached all byte value
        private final @NotNull List<Byte> offsetsByteList;
        private final int blockSize;

        public BlockBuilder(int blockSize) {
            this.dataByteList = new ArrayList<>();
            this.offsetsByteList = new ArrayList<>();
            this.blockSize = blockSize;
        }

        public int estimateSize() {
            return dataByteList.size() + offsetsByteList.size() + SIZE_OF_U16;
        }

        //
        // +-----------------------------------------------------------------+-----+
        // |                           Entry #1                              | ... |
        // +--------------+--------------+----------------+------------------+-----+
        // | key_len(u16) | key(key_len) | value_len(u16) | value(value_len) | ... |
        // +--------------+--------------+----------------+------------------+-----+
        //
        public boolean put(byte @NotNull [] key, byte @NotNull [] value) {
            // NOTICE
            // for reducing call hierarchy check key if empty at engine

            int keyLength = key.length;
            int valueLength = value.length;
            int entryLength = keyLength + valueLength;

            // keyLength + valueLength + offset => 2B + 2B + 2B
            int delta = entryLength + 3 * SIZE_OF_U16;

            if (estimateSize() + delta > blockSize && !isEmpty()) {
                return false;
            }

            offsetsByteList.add((byte) (dataByteList.size() >> 8));
            offsetsByteList.add((byte) dataByteList.size());

            // Todo
            // consider implement key overlap later
            dataByteList.add((byte) (keyLength >> 8));
            dataByteList.add((byte) keyLength);

            for (byte k : key) {
                dataByteList.add(k);
            }

            dataByteList.add((byte) (valueLength >> 8));
            dataByteList.add((byte) valueLength);

            for (byte v : value) {
                dataByteList.add(v);
            }

            return true;
        }

        public boolean isEmpty() {
            return offsetsByteList.isEmpty();
        }

        public @NotNull Block build() {
            if (isEmpty()) {
                throw new IllegalArgumentException("block should not be empty");
            }

            int dataLength = dataByteList.size();
            final byte[] data = new byte[dataLength];
            for (int i = 0; i < dataLength; i++) {
                data[i] = dataByteList.get(i);
            }

            int offsetsLength = offsetsByteList.size() >>> 1;
            final short[] offsets = new short[offsetsLength];
            for (int i = 0; i < offsetsLength; i++) {
                offsets[i] = (short) (offsetsByteList.get(i * SIZE_OF_U16) << 8 | offsetsByteList.get(i * SIZE_OF_U16 + 1) & 0xFF);
            }

            return new Block(data, offsets);
        }
    }

    public static final class BlockIterator {
        private final @NotNull Block block;
        private final byte @NotNull [] firstKey;
        private byte @NotNull [] key;
        private int valueRangeFrom;
        private int valueRangeTo;
        private int index;

        public BlockIterator(@NotNull Block block) {
            this.block = block;
            this.firstKey = block.getFirstKey();
            this.key = EMPTY_BYTE_ARRAY;
            this.valueRangeFrom = 0;
            this.valueRangeTo = 0;
            this.index = 0;
        }

        public static @NotNull BlockIterator createAndSeekToFirst(@NotNull Block block) {
            BlockIterator iter = new BlockIterator(block);
            iter.seekToFirst();
            return iter;
        }

        public static @NotNull BlockIterator createAndSeekToKey(@NotNull Block block, byte @NotNull [] key) {
            BlockIterator iter = new BlockIterator(block);
            iter.seekToKey(key);
            return iter;
        }

        public byte @NotNull [] key() {
            return key;
        }

        public byte @NotNull [] value() {
            int valueLength = valueRangeTo - valueRangeFrom;
            final byte[] buf = new byte[valueLength];
            System.arraycopy(block.data, valueRangeFrom, buf, 0, valueLength);
            return buf;
        }

        public boolean isValid() {
            return key.length != 0;
        }

        public void next() {
            index += 1;
            seekTo(index);
        }

        public void seekTo(int idx) {
            if (idx >= block.offsets.length) {
                key = EMPTY_BYTE_ARRAY;
                valueRangeFrom = 0;
                valueRangeTo = 0;
            } else {
                int offset = block.offsets[idx];
                seekToOffset(offset);
                index = idx;
            }
        }

        public void seekToOffset(int offset) {
            final byte[] data = block.data;
            int cursor = offset;

            int keyLength = data[cursor] << 8 | data[cursor + 1] & 0xFF;
            cursor += 2;

            final byte[] newKey = new byte[keyLength];
            System.arraycopy(data, cursor, newKey, 0, keyLength);
            key = newKey;
            cursor += keyLength;

            int valueLength = data[cursor] << 8 | data[cursor + 1] & 0xFF;
            cursor += 2;

            valueRangeFrom = cursor;
            valueRangeTo = cursor + valueLength;
        }

        public void seekToFirst() {
            seekTo(0);
        }

        // Seek to the first target that >= target
        public void seekToKey(byte @NotNull [] target) {
            int low = 0;
            int high = block.offsets.length;
            while (low < high) {
                int mid = low + (high - low) / 2;
                seekTo(mid);
                // Todo
                // assertion can be removed?
                assert isValid();
                int cmp = Arrays.compare(key, target);
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
