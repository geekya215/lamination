package io.geekya215.lamination;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import static io.geekya215.lamination.Constant.DEFAULT_BLOCK_SIZE;
import static io.geekya215.lamination.Constant.SIZE_OF_U16;

// |        entries        |           offsets         |               |
// |entry|entry|entry|entry|offset|offset|offset|offset|num_of_elements|
public final class Block {
    private final List<Entry> entries;
    private final List<Short> offsets;

    private Block(List<Entry> entries, List<Short> offsets) {
        this.entries = entries;
        this.offsets = offsets;
    }

    public static Block decode(Bytes bytes) {
        byte[] values = bytes.values();
        int length = bytes.length();
        int numOfEntry =
            ((values[length - SIZE_OF_U16] & 0xff) << 8) | (values[length - SIZE_OF_U16 + 1] & 0xff);
        List<Entry> entries = new ArrayList<>(numOfEntry);
        List<Short> offsets = new ArrayList<>(numOfEntry);

        int offsetEndIdx = length - SIZE_OF_U16;
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

    public int estimateSize() {
        return entries.stream().map(Entry::length).reduce(0, Integer::sum)
            + offsets.size() * SIZE_OF_U16
            + SIZE_OF_U16;
    }

    public Bytes getFirstKey() {
        return entries.get(0).key;
    }

    public Bytes encode() {
        List<Byte> bytes = new ArrayList<>(estimateSize());

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
    }
}
