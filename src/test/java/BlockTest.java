import io.geekya215.lamination.Block;
import org.junit.jupiter.api.Test;

import static io.geekya215.lamination.Constants.KB;
import static io.geekya215.lamination.Constants.SIZE_OF_U16;
import static org.junit.jupiter.api.Assertions.*;

public class BlockTest {
    @Test
    void testBuildBlockWithSingleKey() {
        Block.BlockBuilder blockBuilder = new Block.BlockBuilder(16);
        boolean putSuccess = blockBuilder.put("hello".getBytes(), "world".getBytes());
        assertTrue(putSuccess);
        blockBuilder.build();
    }

    @Test
    void testBuildBlockWithFull() {
        Block.BlockBuilder blockBuilder = new Block.BlockBuilder(16);
        assertTrue(blockBuilder.put("1".getBytes(), "11".getBytes()));
        assertFalse(blockBuilder.put("2".getBytes(), "22".getBytes()));
        blockBuilder.build();
    }

    @Test
    void testBuildBlockWithLargeKVPair() {
        Block.BlockBuilder blockBuilder = new Block.BlockBuilder(16);
        assertTrue(blockBuilder.put("1".getBytes(), "1".repeat(100).getBytes()));
        blockBuilder.build();
    }

    byte[] keyOf(int i) {
        return "key_%03d".formatted(i * 5).getBytes();
    }

    byte[] valueOf(int i) {
        return "value_%010d".formatted(i).getBytes();
    }

    Block generateBlock() {
        Block.BlockBuilder blockBuilder = new Block.BlockBuilder(10 * KB);
        for (int i = 0; i < 100; i++) {
            assertTrue(blockBuilder.put(keyOf(i), valueOf(i)));
        }
        return blockBuilder.build();
    }

    @Test
    void testBuildAll() {
        generateBlock();
    }

    @Test
    void testEncodeBlock() {
        Block block = generateBlock();
        byte[] buf = block.encode();
        assertEquals(block.data().length + block.offsets().length * SIZE_OF_U16 + SIZE_OF_U16, buf.length);
    }

    @Test
    void testDecodeBlock() {
        Block block = generateBlock();
        byte[] buf = block.encode();
        Block decodedBlock = Block.decode(buf);
        assertArrayEquals(block.data(), decodedBlock.data());
        assertArrayEquals(block.offsets(), decodedBlock.offsets());
    }

    @Test
    void testBlockIterator() {
        Block block = generateBlock();
        Block.BlockIterator iter = Block.BlockIterator.createAndSeekToFirst(block);
        for (int i = 0; i < 5; i++) {
            for (int j = 0; j < 100; j++) {
                assertTrue(iter.isValid());
                assertArrayEquals(keyOf(j), iter.key());
                assertArrayEquals(valueOf(j), iter.value());
                iter.next();
            }
            iter.seekToFirst();
        }
    }

    @Test
    void testBlockIteratorSeekToKey() {
        Block block = generateBlock();
        Block.BlockIterator iter = Block.BlockIterator.createAndSeekToKey(block, keyOf(0));
        for (int offset = 1; offset <= 5; offset++) {
            for (int i = 0; i < 100; i++) {
                assertTrue(iter.isValid());
                assertArrayEquals(keyOf(i), iter.key());
                assertArrayEquals(valueOf(i), iter.value());
                iter.seekToKey("key_%03d".formatted(i * 5 + offset).getBytes());
            }
            iter.seekToKey("k".getBytes());
        }
    }
}
