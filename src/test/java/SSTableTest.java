import io.geekya215.lamination.Block;
import io.geekya215.lamination.SSTable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SSTableTest {
    Path dir;

    @TempDir
    Path tempDir;

    @BeforeEach
    void setUp() {
        dir = tempDir.resolve("1.sst");
    }

    byte[] keyOf(int i) {
        return String.format("key_%03d", i * 5).getBytes();
    }

    byte[] valueOf(int i) {
        return String.format("value_%010d", i).getBytes();
    }

    @Test
    void testBuildSSTWithSingleKey() throws IOException {
        SSTable.SSTableBuilder builder = new SSTable.SSTableBuilder(16);
        builder.put("22".getBytes(), "33".getBytes());
        SSTable sst = builder.build(0, dir);
    }

    @Test
    void testBuildSSTWithTwoBlocks() throws IOException {
        SSTable.SSTableBuilder builder = new SSTable.SSTableBuilder(16);
        builder.put("11".getBytes(), "11".getBytes());
        builder.put("22".getBytes(), "22".getBytes());
        builder.put("33".getBytes(), "33".getBytes());
        builder.put("44".getBytes(), "44".getBytes());
        builder.put("55".getBytes(), "55".getBytes());
        builder.put("66".getBytes(), "66".getBytes());
        SSTable sst = builder.build(0, dir);
    }

    @Test
    SSTable generateSST() throws IOException {
        SSTable.SSTableBuilder builder = new SSTable.SSTableBuilder(128);
        for (int i = 0; i < 100; i++) {
            builder.put(keyOf(i), valueOf(i));
        }
        SSTable sst = builder.build(0, dir);
        return sst;
    }

    @Test
    void testDecodeSST() throws IOException {
        SSTable sst = generateSST();
        List<SSTable.MetaBlock> metaBlocks = sst.metaBlocks();
        SSTable open = SSTable.open(0, sst.file());
        assertEquals(metaBlocks, open.metaBlocks());
    }

    @Test
    void testSSTIterator() throws IOException {
        SSTable sst = generateSST();
        SSTable.SSTableIterator iterator = SSTable.SSTableIterator.createAndSeekToFirst(sst);
        for (int i = 0; i < 5; i++) {
            int index = 0;
            while (iterator.hasNext()) {
                Block block = iterator.next();
                Block.BlockIterator blockIterator = Block.BlockIterator.createAndSeekToFirst(block);
                while (blockIterator.hasNext()) {
                    Block.Entry entry = blockIterator.next();
                    assertEquals(new Block.Entry(keyOf(index), valueOf(index)), entry);
                    index++;
                }
            }
            iterator.seekToFirst();
        }
    }

    @Test
    void testSSTIteratorSeekToKey() throws IOException {
        SSTable sst = generateSST();
        for (int offset = 0; offset < 5; offset++) {
            for (int i = 0; i < 100; i++) {
                byte[] key = String.format("key_%03d", i * 5 + offset).getBytes();
                SSTable.SSTableIterator sstIterator = SSTable.SSTableIterator.createAndSeekToKey(sst, key);
                Block block = sstIterator.next();
                Block.BlockIterator blockIterator = Block.BlockIterator.createAndSeekToKey(block, keyOf(i));
                Block.Entry entry = blockIterator.next();
                assertEquals(new Block.Entry(keyOf(i), valueOf(i)), entry);
            }
        }
    }
}
