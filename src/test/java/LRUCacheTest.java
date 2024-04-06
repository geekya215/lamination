import io.geekya215.lamination.LRUCache;
import io.geekya215.lamination.Measurable;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class LRUCacheTest {
    record Entry(byte @NotNull [] key, byte @NotNull [] value) implements Measurable {
        @Override
        public int estimateSize() {
            return key.length + value.length;
        }
    }

    @Test
    void testLRUCacheGet() {
        LRUCache<Integer, Entry> cache = new LRUCache<>(1024);
        Entry notExist = cache.get(1);
        assertNull(notExist);
        cache.put(1, new Entry("key1".getBytes(), "value1".getBytes()));
        Entry exist = cache.get(1);
        assertNotNull(exist);
        assertArrayEquals("key1".getBytes(), exist.key);
        assertArrayEquals("value1".getBytes(), exist.value);
    }

    @Test
    void testLRUCacheEvict() {
        LRUCache<Integer, Entry> cache = new LRUCache<>(32);
        for (int i = 1; i < 4; i++) {
            cache.put(i, new Entry("key%d".formatted(i).getBytes(), "value%d".formatted(i).getBytes()));
        }
        assertNotNull(cache.get(1));
        cache.put(4, new Entry("key4".getBytes(), "value4".getBytes()));
        assertNull(cache.get(2));
    }
}
