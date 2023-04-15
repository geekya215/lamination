import io.geekya215.lamination.Block;
import io.geekya215.lamination.Bytes;
import org.junit.jupiter.api.Assertions;
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
        assertTrue(builder.put(Bytes.of("22".getBytes()), Bytes.of("333".getBytes())));
        builder.build();
    }

    @Test
    void testBuildBlockFull() {
        Block.BlockBuilder builder = new Block.BlockBuilder(16);
        assertTrue(builder.put(Bytes.of("22".getBytes()), Bytes.of("333".getBytes())));
        assertFalse(builder.put(Bytes.of("22".getBytes()), Bytes.of("333".getBytes())));
        builder.build();
    }

    Block generateBlock() {
        Block.BlockBuilder builder = new Block.BlockBuilder(10000);
        for (int i = 0; i < 100; i++) {
            byte[] key = keyOf(i);
            byte[] value = valueOf(i);
            Assertions.assertTrue(builder.put(Bytes.of(key), Bytes.of(value)));
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
        Bytes encode = block.encode();
        Block decodeBlock = Block.decode(encode);
        Assertions.assertEquals(block.getOffsets(), decodeBlock.getOffsets());
        Assertions.assertEquals(block.getEntries(), decodeBlock.getEntries());
    }

    @Test
    void testBlockIterator() {
        Block block = generateBlock();
        Block.BlockIterator iter = new Block.BlockIterator(block);
        int i = 0;
        while (iter.hasNext()) {
            Block.Entry entry = iter.next();
            assertEquals(Block.Entry.of(Bytes.of(keyOf(i)), Bytes.of(valueOf(i))), entry);
            i++;
        }

        for (int j = 0; j < 100; j++) {
            Block.Entry entry = iter.seekTo(j);
            assertEquals(Block.Entry.of(Bytes.of(keyOf(j)), Bytes.of(valueOf(j))), entry);
        }
    }

    @Test
    void testBlockIteratorSeekToKey() {
        Block block = generateBlock();
        Block.BlockIterator iter = new Block.BlockIterator(block);
        for (int i = 0; i < 100; i += 5) {
            Block.Entry entry = iter.seekToKey(Bytes.of(keyOf(i)));
            assertEquals(Bytes.of(keyOf(i)), entry.key());
        }

        assertThrows(NoSuchElementException.class, () -> {
            iter.seekToKey(Bytes.of(keyOf(114514)));
        });
    }
}
