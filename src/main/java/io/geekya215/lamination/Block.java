package io.geekya215.lamination;

import java.util.ArrayList;
import java.util.List;

// |        entries        |           offsets         |               |
// |entry|entry|entry|entry|offset|offset|offset|offset|num_of_elements|
public final class Block {
    static final int SIZE_OF_U16 = 2;
    static final int DEFAULT_BLOCK_SIZE = 1024;

    private final List<Entry> entries;
    private final List<Short> offsets;

    private Block(List<Entry> entries, List<Short> offsets) {
        this.entries = entries;
        this.offsets = offsets;
    }

    public List<Entry> getEntries() {
        return entries;
    }

    public List<Short> getOffsets() {
        return offsets;
    }

    public Bytes encode() {
        int entriesSize = entries.stream().map(Entry::length).reduce(0, Integer::sum);
        int offsetsSize = offsets.size() * SIZE_OF_U16;
        List<Byte> bytes = new ArrayList<>(entriesSize + offsetsSize + SIZE_OF_U16);

        for (Entry entry : entries) {
            int keyLength = entry.keyLength();
            int valueLength = entry.valueLength();

            bytes.add((byte) (keyLength >> 8));
            bytes.add((byte) keyLength);

            bytes.add((byte) (valueLength >> 8));
            bytes.add((byte) valueLength);

            for (byte k : entry.key.values()) {
                bytes.add(k);
            }

            for (byte v : entry.value.values()) {
                bytes.add(v);
            }
        }

        for (short offset : offsets) {
            bytes.add((byte) (offset >> 8));
            bytes.add((byte) offset);
        }

        int numOfEntries = entries.size();
        bytes.add((byte) (numOfEntries >> 8));
        bytes.add((byte) numOfEntries);

        return Bytes.of(bytes);
    }

    public static Block decode(Bytes bytes) {
        byte[] values = bytes.values();
        int length = bytes.length();
        int numOfEntry =
            ((values[length - SIZE_OF_U16] & 0xff) << 8) + (values[length - SIZE_OF_U16 + 1] & 0xff);
        List<Entry> entries = new ArrayList<>(numOfEntry);
        List<Short> offsets = new ArrayList<>(numOfEntry);

        int offsetEndIdx = length - SIZE_OF_U16;
        int entriesEndIdx = offsetEndIdx - numOfEntry * SIZE_OF_U16;

        for (int i = entriesEndIdx; i < offsetEndIdx; i += 2) {
            short offset = (short) (((values[i] & 0xff) << 8) | (values[i + 1] & 0xff));
            offsets.add(offset);
        }

        for (int i = 0; i < entriesEndIdx; ) {
            int keyLength = ((values[i] & 0xff) << 8) + (values[i + 1] & 0xff);
            int valueLength = ((values[i + 2] & 0xff) << 8) + (values[i + 3] & 0xff);

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

    // |                             entry                             |
    // | key_len (2B) | value_len (2B) | key (keylen) | value (varlen) |
    record Entry(Bytes key, Bytes value) implements Comparable<Entry> {
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
        private int currentSize;
        private final int blockSize;

        public BlockBuilder() {
            this(DEFAULT_BLOCK_SIZE);
        }

        public BlockBuilder(int blockSize) {
            this.entries = new ArrayList<>();
            this.offsets = new ArrayList<>();
            this.currentSize = 0;
            this.blockSize = blockSize;
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

        public Block build() {
            if (entries.size() == 0) {
                throw new IllegalArgumentException("block should not be empty");
            }
            return new Block(entries, offsets);
        }
    }
}
