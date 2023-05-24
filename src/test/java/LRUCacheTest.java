import io.geekya215.lamination.Block;
import io.geekya215.lamination.LRUCache;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class LRUCacheTest {
    LRUCache<Integer, Block> lruCache;
    Block.BlockBuilder blockBuilder;

    @BeforeEach
    void setup() {
        lruCache = new LRUCache<>(100);
        blockBuilder = new Block.BlockBuilder(30);
    }

    byte[] generateKey(int i) {
        return String.format("key_%03d", i).getBytes();
    }

    byte[] generateValue(int i) {
        return String.format("value_%03d", i).getBytes();
    }

    @Test
    void testNormal() {
        blockBuilder.put(generateKey(0), generateValue(0));
        blockBuilder.put(generateKey(1), generateValue(1));
        Block block1 = blockBuilder.build();
        lruCache.put(1, block1);
        blockBuilder.reset();

        blockBuilder.put(generateKey(2), generateValue(2));
        blockBuilder.put(generateKey(3), generateValue(3));
        Block block2 = blockBuilder.build();
        lruCache.put(2, block2);

        assertEquals(block1, lruCache.get(1));
        assertEquals(block2, lruCache.get(2));
    }

    @Test
    void testPutOverFlow() {
        blockBuilder.put(generateKey(0), generateValue(0));
        blockBuilder.put(generateKey(1), generateValue(1));
        Block block1 = blockBuilder.build();
        lruCache.put(1, block1);
        blockBuilder.reset();

        blockBuilder.put(generateKey(2), generateValue(2));
        blockBuilder.put(generateKey(3), generateValue(3));
        Block block2 = blockBuilder.build();
        lruCache.put(2, block2);
        blockBuilder.reset();

        blockBuilder.put(generateKey(4), generateValue(4));
        blockBuilder.put(generateKey(5), generateValue(5));
        Block block3 = blockBuilder.build();
        lruCache.put(3, block3);
        blockBuilder.reset();

        blockBuilder.put(generateKey(6), generateValue(6));
        blockBuilder.put(generateKey(7), generateValue(7));
        Block block4 = blockBuilder.build();
        lruCache.put(4, block4);

        assertNull(lruCache.get(1));
    }

    @Test
    void testGetChange() {
        blockBuilder.put(generateKey(0), generateValue(0));
        blockBuilder.put(generateKey(1), generateValue(1));
        Block block1 = blockBuilder.build();
        lruCache.put(1, block1);
        blockBuilder.reset();

        blockBuilder.put(generateKey(2), generateValue(2));
        blockBuilder.put(generateKey(3), generateValue(3));
        Block block2 = blockBuilder.build();
        lruCache.put(2, block2);
        blockBuilder.reset();

        blockBuilder.put(generateKey(4), generateValue(4));
        blockBuilder.put(generateKey(5), generateValue(5));
        Block block3 = blockBuilder.build();
        lruCache.put(3, block3);
        blockBuilder.reset();

        assertEquals(block1, lruCache.get(1));
        blockBuilder.put(generateKey(6), generateValue(6));
        blockBuilder.put(generateKey(7), generateValue(7));
        Block block4 = blockBuilder.build();
        lruCache.put(4, block4);

        assertNull(lruCache.get(2));
    }
}
