import io.geekya215.lamination.Block;
import org.junit.jupiter.api.Test;

import java.util.NoSuchElementException;

import static org.junit.jupiter.api.Assertions.*;

public class BlockTest {
    byte[] keyOf(int i) {
        return String.format("key_%03d", i * 5).getBytes();
    }

    byte[] valueOf(int i) {
        return String.format("value_%010d", i).getBytes();
    }

    @Test
    void testBuildBlockWithSingleKey() {
        Block.BlockBuilder builder = new Block.BlockBuilder(16);
        assertTrue(builder.put("22".getBytes(), "33".getBytes()));
        builder.build();
    }

    @Test
    void testBuildBlockFull() {
        Block.BlockBuilder builder = new Block.BlockBuilder(16);
        assertTrue(builder.put("22".getBytes(), "33".getBytes()));
        assertFalse(builder.put("22".getBytes(), "33".getBytes()));
        builder.build();
    }

    Block generateBlock() {
        Block.BlockBuilder builder = new Block.BlockBuilder(10000);
        for (int i = 0; i < 100; i++) {
            byte[] key = keyOf(i);
            byte[] value = valueOf(i);
            assertTrue(builder.put(key, value));
        }

        return builder.build();
    }

    @Test
    void testBlockBuildAll() {
        generateBlock();
    }

    @Test
    void testBlockEncode() {
        Block block = generateBlock();
        block.encode();
    }

    @Test
    void testBlockDecode() {
        Block block = generateBlock();
        byte[] encode = block.encode();
        Block decodeBlock = Block.decode(encode);
        assertEquals(block.offsets(), decodeBlock.offsets());
        assertEquals(block.entries(), decodeBlock.entries());
    }

    @Test
    void testBlockIterator() {
        Block block = generateBlock();
        Block.BlockIterator iter = new Block.BlockIterator(block);
        int i = 0;
        while (iter.hasNext()) {
            Block.Entry entry = iter.next();
            assertEquals(new Block.Entry(keyOf(i), valueOf(i)), entry);
            i++;
        }

        for (int j = 0; j < 100; j++) {
            Block.Entry entry = iter.seekTo(j);
            assertEquals(new Block.Entry(keyOf(j), valueOf(j)), entry);
        }
    }

    @Test
    void testBlockIteratorSeekToKey() {
        Block block = generateBlock();
        Block.BlockIterator iter = new Block.BlockIterator(block);
        for (int i = 0; i < 100; i += 5) {
            Block.Entry entry = iter.seekToKey(keyOf(i));
            assertArrayEquals(keyOf(i), entry.key());
        }

        assertThrows(NoSuchElementException.class, () -> {
            iter.seekToKey(keyOf(114514));
        });
    }
}
