package io.geekya215.lamination;

import java.util.*;
import java.util.zip.CRC32;

import static io.geekya215.lamination.Constant.*;

//  --------------------------------------------------------------------
// |                            Block Size                              |
// |--------------------------------------------------------------------|
// |         |                   |   offsets(2B/per)  |     entries     |
// |crc32(4B)|num_of_elements(2B)|offset|offset|offset|entry|entry|entry|
//  --------------------------------------------------------------------
public record Block(List<Short> offsets, List<Entry> entries) implements Measurable {
    private static final CRC32 crc32 = new CRC32();

    public byte[] encode() {
        int blockSize = estimateSize();
        byte[] bytes = new byte[blockSize];
        int index = 4;

        // num of elements
        int numOfElements = offsets.size();
        bytes[index++] = (byte) (numOfElements >> 8);
        bytes[index++] = (byte) numOfElements;

        // offsets
        for (short offset : offsets) {
            bytes[index++] = (byte) (offset >> 8);
            bytes[index++] = (byte) offset;
        }

        // entries
        for (Entry entry : entries) {
            int keySize = entry.keySize();
            int valueSize = entry.valueSize();

            bytes[index++] = (byte) (keySize >> 8);
            bytes[index++] = (byte) keySize;

            bytes[index++] = (byte) (valueSize >> 8);
            bytes[index++] = (byte) valueSize;

            for (byte k : entry.key) {
                bytes[index++] = k;
            }

            for (byte v : entry.value) {
                bytes[index++] = v;
            }
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
        int crc = (int) crc32.getValue();
        crc32.reset();

        //  -------------------------
        // | crc32 | num_of_elements |
        // |  4(B) |       2(B)      |
        // ^-------^-----------------^
        // 0       4                 6
        int checkSum = (bytes[0] & 0xff) << 24
            | (bytes[1] & 0xff) << 16
            | (bytes[2] & 0xff) << 8
            | bytes[3] & 0xff;

        if (crc != checkSum) {
            throw new RuntimeException("check sum not equal");
        }

        int numOfElements = (bytes[4] & 0xff) << 8 | (bytes[5] & 0xff);
        List<Short> offsets = new ArrayList<>(numOfElements);
        List<Entry> entries = new ArrayList<>(numOfElements);

        int index = 6;

        for (int i = 0; i < numOfElements; i++) {
            short offset = (short) (((bytes[index] & 0xff) << 8) | (bytes[index + 1] & 0xff));
            offsets.add(offset);
            index += 2;
        }

        for (int i = 0; i < numOfElements; i++) {
            int keySize = (((bytes[index] & 0xff) << 8) | (bytes[index + 1] & 0xff));
            index += 2;
            int valueSize = (((bytes[index] & 0xff) << 8) | (bytes[index + 1] & 0xff));
            index += 2;

            byte[] key = new byte[keySize];
            byte[] value = new byte[valueSize];

            System.arraycopy(bytes, index, key, 0, keySize);
            index += keySize;
            System.arraycopy(bytes, index, value, 0, valueSize);
            index += valueSize;

            entries.add(new Entry(key, value));
        }

        return new Block(offsets, entries);
    }

    public byte[] firstKey() {
        return entries.get(0).key;
    }

    @Override
    public int estimateSize() {
        return SIZE_OF_U32 // crc32
            + SIZE_OF_U16 // num of elements
            + offsets.size() * SIZE_OF_U16 // offsets
            + entries.stream().map(Entry::estimateSize).reduce(0, Integer::sum); // entries
    }

    //  ---------------------------------------------------------------
    // |                             entry                             |
    // |---------------------------------------------------------------|
    // | key_len (2B) | value_len (2B) | key (keylen) | value (varlen) |
    //  ---------------------------------------------------------------
    public record Entry(byte[] key, byte[] value) implements Comparable<Entry>, Measurable {
        public int keySize() {
            return key.length;
        }

        public int valueSize() {
            return value.length;
        }

        @Override
        public int estimateSize() {
            return 2 * SIZE_OF_U16 + keySize() + valueSize();
        }

        @Override
        public int compareTo(Entry o) {
            return Arrays.compare(key, o.key);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Entry entry = (Entry) o;

            if (!Arrays.equals(key, entry.key)) return false;
            return Arrays.equals(value, entry.value);
        }

        @Override
        public int hashCode() {
            int result = Arrays.hashCode(key);
            result = 31 * result + Arrays.hashCode(value);
            return result;
        }
    }

    public static final class BlockBuilder {
        private final List<Short> offsets;
        private final List<Entry> entries;
        private final int blockSize;
        private int currentSize;

        public BlockBuilder() {
            this(DEFAULT_BLOCK_SIZE);
        }

        public BlockBuilder(int blockSize) {
            this.offsets = new ArrayList<>();
            this.entries = new ArrayList<>();
            this.blockSize = blockSize;
            this.currentSize = 0;
        }

        public boolean put(byte[] key, byte[] value) {
            int keySize = key.length;
            if (keySize == 0) {
                throw new IllegalArgumentException("key must not be empty");
            }

            int valueSize = value.length;
            int entrySize = 2 * SIZE_OF_U16 + keySize + valueSize;

            // entry size + num of elements(2B) + crc(4B)
            if (currentSize + entrySize + SIZE_OF_U32 + SIZE_OF_U16 > blockSize) {
                return false;
            }

            offsets.add((short) currentSize);
            entries.add(new Entry(key, value));
            currentSize += entrySize;

            return true;
        }

        public void reset() {
            offsets.clear();
            entries.clear();
            currentSize = 0;
        }

        public Block build() {
            if (offsets.size() == 0) {
                throw new IllegalArgumentException("block should not be empty");
            }
            return new Block(List.copyOf(offsets), List.copyOf(entries));
        }
    }

    public static final class BlockIterator implements Iterator<Entry> {
        private final List<Entry> entries;
        private int entryIndex;

        public BlockIterator(Block block) {
            this.entries = block.entries;
            this.entryIndex = 0;
        }

        public static BlockIterator createAndSeekToFirst(Block block) {
            BlockIterator blockIterator = new BlockIterator(block);
            return blockIterator;
        }

        public static BlockIterator createAndSeekToKey(Block block, byte[] key) {
            BlockIterator blockIterator = new BlockIterator(block);
            blockIterator.seekToKey(key);
            return blockIterator;
        }

        public Entry entry() {
            return entries.get(entryIndex);
        }

        public Entry seekTo(int index) {
            if (index >= entries.size()) {
                throw new NoSuchElementException();
            }
            return entries.get(entryIndex = index);
        }

        public Entry seekToKey(byte[] key) {
            int low = 0;
            int high = entries.size();

            while (low < high) {
                int mid = low + (high - low) / 2;
                Entry entry = entries.get(mid);
                int cmp = Arrays.compare(entry.key, key);
                if (cmp < 0) {
                    low = mid + 1;
                } else if (cmp == 0) {
                    entryIndex = mid;
                    return entry;
                } else {
                    high = mid;
                }
            }

            throw new NoSuchElementException("could not find key " + Arrays.toString(key));
        }

        @Override
        public boolean hasNext() {
            return entryIndex < entries.size();
        }

        @Override
        public Entry next() {
            if (entryIndex >= entries.size()) {
                throw new NoSuchElementException();
            }
            return entries.get(entryIndex++);
        }
    }
}
