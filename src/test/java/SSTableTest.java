import io.geekya215.lamination.Block;
import io.geekya215.lamination.LRUCache;
import io.geekya215.lamination.SSTable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class SSTableTest {
    @TempDir
    File tempDir;

    LRUCache<Long, Block> blockCache;

    @BeforeEach
    void setup() {
        blockCache = new LRUCache<>();
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
        builder.build(0, tempDir, blockCache);
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
        builder.build(0, tempDir, blockCache);
    }

    @Test
    SSTable generateSST() throws IOException {
        SSTable.SSTableBuilder builder = new SSTable.SSTableBuilder(128);
        for (int i = 0; i < 100; i++) {
            builder.put(keyOf(i), valueOf(i));
        }
        return builder.build(0, tempDir, blockCache);
    }

    @Test
    void testDecodeSST() throws IOException {
        SSTable sst = generateSST();
        List<SSTable.MetaBlock> metaBlocks = sst.getMetaBlocks();
        SSTable open = SSTable.open(0, tempDir, blockCache);
        assertEquals(metaBlocks, open.getMetaBlocks());
    }

    @Test
    void testSSTIterator() throws IOException {
        SSTable sst = generateSST();
        SSTable.SSTableIterator iterator = SSTable.SSTableIterator.createAndSeekToFirst(sst);
        for (int i = 0; i < 5; i++) {
            for (int j = 0; j < 100; j++) {
                byte[] key = iterator.key();
                byte[] value = iterator.value();
                assertArrayEquals(keyOf(j), key);
                assertArrayEquals(valueOf(j), value);
                iterator.next();
            }
            iterator.seekToFirst();
        }
    }

    @Test
    void testSSTIteratorSeekToKey() throws IOException {
        SSTable sst = generateSST();
        SSTable.SSTableIterator iterator = SSTable.SSTableIterator.createAndSeekToKey(sst, keyOf(0));
        for (int offset = 1; offset <= 5; offset++) {
            for (int i = 0; i < 100; i++) {
                iterator.seekToKey(keyOf(i));
                byte[] key = iterator.key();
                byte[] value = iterator.value();
                assertArrayEquals(keyOf(i), key);
                assertArrayEquals(valueOf(i), value);
                iterator.seekToKey(String.format("key_%03d", i * 5 + offset).getBytes());
            }
            iterator.seekToKey("k".getBytes());
        }
    }
}
