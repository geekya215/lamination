package iterators;

import io.geekya215.lamination.iterators.StorageIterator;

import java.util.List;

public final class MockIterator implements StorageIterator {
    private int index;
    private final List<Pair<byte[], byte[]>> data;

    public MockIterator(List<Pair<byte[], byte[]>> data) {
        this.index = 0;
        this.data = data;
    }

    @Override
    public byte[] key() {
        return data.get(index).fst();
    }

    @Override
    public byte[] value() {
        return data.get(index).snd();
    }

    @Override
    public boolean isValid() {
        return index < data.size();
    }

    @Override
    public void next() {
        if (index < data.size()) {
            index += 1;
        }
    }
}
