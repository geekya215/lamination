import io.geekya215.lamination.StorageIterator;
import io.geekya215.lamination.tuple.Tuple2;
import org.jetbrains.annotations.NotNull;

import java.util.List;

public final class MockIterator implements StorageIterator {
    private final @NotNull List<Tuple2<byte[], byte[]>> data;
    private int current;

    public MockIterator(@NotNull List<Tuple2<byte[], byte[]>> data) {
        this.data = data;
        this.current = 0;
    }

    @Override
    public byte @NotNull [] key() {
        return data.get(current).t1();
    }

    @Override
    public byte @NotNull [] value() {
        return data.get(current).t2();
    }

    @Override
    public boolean isValid() {
        return current < data.size();
    }

    @Override
    public void next() {
        if (current < data.size()) {
            current += 1;
        }
    }
}
