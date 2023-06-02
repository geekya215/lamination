package io.geekya215.lamination.iterators;

import java.io.IOException;
import java.util.*;

public final class MergeIterator implements StorageIterator {
    private final Iterator<Entry> iter;
    private Entry current;

    record Entry(byte[] key, byte[] value) implements Comparable<Entry> {
        @Override
        public int compareTo(Entry o) {
            return Arrays.compare(key, o.key);
        }
    }

    public static MergeIterator create(List<StorageIterator> iters) throws IOException {
        SortedSet<Entry> set = new TreeSet<>(Entry::compareTo);
        for (StorageIterator iter : iters) {
            while (iter.isValid()) {
                set.add(new Entry(iter.key(), iter.value()));
                iter.next();
            }
        }
        return new MergeIterator(set);
    }

    public MergeIterator(SortedSet<Entry> set) {
        this.iter = set.iterator();
        this.current = this.iter.hasNext() ? this.iter.next() : null;
    }

    @Override
    public byte[] key() {
        return current.key;
    }

    @Override
    public byte[] value() {
        return current.value;
    }

    @Override
    public boolean isValid() {
        return current != null;
    }

    @Override
    public void next() throws IOException {
        current = iter.hasNext() ? iter.next() : null;
    }
}
