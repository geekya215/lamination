package io.geekya215.lamination;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.zip.CRC32;

import static io.geekya215.lamination.Constant.*;

// |     entries     |       offsets      |                   |         |
// |entry|entry|entry|offset|offset|offset|num_of_elements(2B)|crc32(4B)|
public final class Block {
    private final List<Entry> entries;
    private final List<Short> offsets;
    private static final CRC32 crc = new CRC32();

    private Block(List<Entry> entries, List<Short> offsets) {
        this.entries = entries;
        this.offsets = offsets;
    }

    public Bytes encode() {
        int blockSize = estimateSize();
        byte[] bytes = new byte[blockSize];
        int index = 0;

        for (Entry entry : entries) {
            int keyLength = entry.keyLength();
            int valueLength = entry.valueLength();

            bytes[index++] = (byte) (keyLength >> 8);
            bytes[index++] = (byte) keyLength;

            bytes[index++] = (byte) (valueLength >> 8);
            bytes[index++] = (byte) valueLength;

            for (byte k : entry.key.values()) {
                bytes[index++] = k;
            }

            for (byte v : entry.value.values()) {
                bytes[index++] = v;
            }
        }

        for (short offset : offsets) {
            bytes[index++] = (byte) (offset >> 8);
            bytes[index++] = (byte) offset;
        }

        int numOfElements = entries.size();
        bytes[index++] = (byte) (numOfElements >> 8);
        bytes[index++] = (byte) numOfElements;

        crc.update(bytes, 0, blockSize - 4);
        int checkSum = (int) crc.getValue();
        bytes[index++] = (byte) (checkSum >> 24);
        bytes[index++] = (byte) (checkSum >> 16);
        bytes[index++] = (byte) (checkSum >> 8);
        bytes[index] = (byte) checkSum;
        crc.reset();

        return Bytes.of(bytes);
    }

    public static Block decode(Bytes bytes) {
        byte[] values = bytes.values();
        int length = bytes.length();

        crc.update(values, 0, length - 4);
        int checkSum = (int) crc.getValue();
        crc.reset();

        int crc32 = (values[length - 4] & 0xff) << 24
            | (values[length - 3] & 0xff) << 16
            | (values[length - 2] & 0xff) << 8
            | values[length - 1] & 0xff;

        if (crc32 != checkSum) {
            throw new RuntimeException("checksum not equal");
        }

        int numOfElementsEndIdx = length - 4;

        int numOfEntry =
            ((values[numOfElementsEndIdx - SIZE_OF_U16] & 0xff) << 8)
                | (values[numOfElementsEndIdx - SIZE_OF_U16 + 1] & 0xff);
        List<Entry> entries = new ArrayList<>(numOfEntry);
        List<Short> offsets = new ArrayList<>(numOfEntry);

        int offsetEndIdx = numOfElementsEndIdx - SIZE_OF_U16;
        int entriesEndIdx = offsetEndIdx - numOfEntry * SIZE_OF_U16;

        for (int i = entriesEndIdx; i < offsetEndIdx; i += 2) {
            short offset = (short) (((values[i] & 0xff) << 8) | (values[i + 1] & 0xff));
            offsets.add(offset);
        }

        for (int i = 0; i < entriesEndIdx; ) {
            int keyLength = ((values[i] & 0xff) << 8) | (values[i + 1] & 0xff);
            int valueLength = ((values[i + 2] & 0xff) << 8) | (values[i + 3] & 0xff);

            int keyStartIdx = i + 4;
            int keyEndIdx = i + 4 + keyLength;
            int valueEndIdx = keyEndIdx + valueLength;

            Bytes key = bytes.slice(keyStartIdx, keyEndIdx);
            Bytes value = bytes.slice(keyEndIdx, valueEndIdx);

            i += (4 + keyLength + valueLength);

            entries.add(new Entry(key, value));
        }

        return new Block(entries, offsets);
    }

    public List<Entry> getEntries() {
        return entries;
    }

    public List<Short> getOffsets() {
        return offsets;
    }

    public Bytes getFirstKey() {
        return entries.get(0).key;
    }

    public int estimateSize() {
        return entries.stream().map(Entry::length).reduce(0, Integer::sum) // entries
            + offsets.size() * SIZE_OF_U16 // offsets
            + SIZE_OF_U16 // num of entries
            + SIZE_OF_U32; // crc32
    }

    // |                             entry                             |
    // | key_len (2B) | value_len (2B) | key (keylen) | value (varlen) |
    public record Entry(Bytes key, Bytes value) implements Comparable<Entry> {
        public static Entry of(Bytes key, Bytes value) {
            return new Entry(key, value);
        }

        int keyLength() {
            return key.length();
        }

        int valueLength() {
            return value.length();
        }

        int length() {
            return 2 * SIZE_OF_U16 + keyLength() + valueLength();
        }

        @Override
        public int compareTo(Entry o) {
            return key.compareTo(o.key);
        }
    }

    public static final class BlockBuilder {
        private final List<Entry> entries;
        private final List<Short> offsets;
        private final int blockSize;
        private int currentSize;

        public BlockBuilder() {
            this(DEFAULT_BLOCK_SIZE);
        }

        public BlockBuilder(int blockSize) {
            this.entries = new ArrayList<>();
            this.offsets = new ArrayList<>();
            this.blockSize = blockSize;
            this.currentSize = 0;
        }

        public boolean put(Bytes key, Bytes value) {
            int keyLength = key.length();
            if (keyLength == 0) {
                throw new IllegalArgumentException("key must not be empty");
            }

            int valueLength = value.length();
            int entrySize = 2 * SIZE_OF_U16 + keyLength + valueLength;
            if (currentSize + entrySize + SIZE_OF_U16 > blockSize) {
                return false;
            }

            entries.add(new Entry(key, value));
            offsets.add((short) currentSize);
            currentSize += entrySize;

            return true;
        }

        public void reset() {
            entries.clear();
            offsets.clear();
            currentSize = 0;
        }

        public Block build() {
            if (entries.size() == 0) {
                throw new IllegalArgumentException("block should not be empty");
            }
            return new Block(List.copyOf(entries), List.copyOf(offsets));
        }
    }

    public static final class BlockIterator implements Iterator<Entry> {
        private final List<Entry> entries;
        private int cursor;

        public BlockIterator(Block block) {
            this.entries = block.entries;
            this.cursor = 0;
        }

        public static BlockIterator createAndSeekToFirst(Block block) {
            BlockIterator blockIterator = new BlockIterator(block);
            blockIterator.seekTo(0);
            return blockIterator;
        }

        public Entry seekTo(int index) {
            if (index >= entries.size()) {
                throw new NoSuchElementException();
            }
            return entries.get(cursor = index);
        }

        public Entry seekToKey(Bytes key) {
            int low = 0;
            int high = entries.size();

            while (low < high) {
                int mid = low + (high - low) / 2;
                Entry entry = entries.get(mid);
                int cmp = entry.key.compareTo(key);
                if (cmp < 0) {
                    low = mid + 1;
                } else if (cmp == 0) {
                    return entry;
                } else {
                    high = mid;
                }
            }

            throw new NoSuchElementException("could not find key " + key);
        }

        @Override
        public boolean hasNext() {
            return cursor < entries.size();
        }

        @Override
        public Entry next() {
            if (cursor >= entries.size()) {
                throw new NoSuchElementException();
            }
            return entries.get(cursor++);
        }
    }
}
