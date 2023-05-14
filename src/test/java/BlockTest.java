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
    void testBuildEmptyBlock() {
        Block.BlockBuilder builder = new Block.BlockBuilder(16);
        assertThrows(IllegalStateException.class, builder::build);
    }

    @Test
    void testBuildBlockWithLargeKey() {
        Block.BlockBuilder builder = new Block.BlockBuilder(16);
        assertThrows(IllegalArgumentException.class, () -> builder.put("114514".getBytes(), "1919810".getBytes()));
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
        assertArrayEquals(block.getOffsets(), decodeBlock.getOffsets());
        assertArrayEquals(block.getData(), decodeBlock.getData());
    }

    @Test
    void testBlockIterator() {
        Block block = generateBlock();
        Block.BlockIterator iter = Block.BlockIterator.createAndSeekToFirst(block);
        int i = 0;
        while (iter.isValid()) {
            byte[] key = iter.getKey();
            byte[] value = iter.getValue();
            assertArrayEquals(keyOf(i), key);
            assertArrayEquals(valueOf(i), value);
            iter.next();
            i++;
        }

        for (int j = 0; j < 100; j++) {
            iter.seekTo(j);
            byte[] key = iter.getKey();
            byte[] value = iter.getValue();
            assertArrayEquals(keyOf(j), key);
            assertArrayEquals(valueOf(j), value);
        }
    }

    @Test
    void testBlockIteratorSeekToKey() {
        Block block = generateBlock();
        Block.BlockIterator iter = new Block.BlockIterator(block);
        for (int i = 0; i < 100; i += 5) {
            iter.seekToKey(keyOf(i));
            byte[] key = iter.getKey();
            byte[] value = iter.getValue();
            assertArrayEquals(keyOf(i), key);
            assertArrayEquals(valueOf(i), value);
        }

        assertThrows(NoSuchElementException.class, () -> iter.seekToKey(keyOf(114514)));
    }
}
