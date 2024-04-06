import io.geekya215.lamination.Engine;
import io.geekya215.lamination.LRUCache;
import io.geekya215.lamination.SortedStringTable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;

import static io.geekya215.lamination.Constants.KB;
import static org.junit.jupiter.api.Assertions.*;

public class SortedStringTableTest {
    @TempDir
    Path tempDir;

    @Test
    void testBuildSSTWithSingleKey() throws IOException {
        SortedStringTable.SortedStringTableBuilder sstBuilder = new SortedStringTable.SortedStringTableBuilder(16);
        sstBuilder.put("1".getBytes(), "1.1".getBytes());
        SortedStringTable sst = sstBuilder.build(0, new LRUCache<>(KB), Engine.getPathOfSST(tempDir, 0));
        sst.getFile().close();
    }

    @Test
    void testBuildSSTWithMultiBlock() throws IOException {
        SortedStringTable.SortedStringTableBuilder sstBuilder = new SortedStringTable.SortedStringTableBuilder(16);
        sstBuilder.put("1".getBytes(), "1.1".getBytes());
        sstBuilder.put("2".getBytes(), "2.1".getBytes());
        sstBuilder.put("3".getBytes(), "3.1".getBytes());
        sstBuilder.put("4".getBytes(), "4.1".getBytes());
        sstBuilder.put("5".getBytes(), "5.1".getBytes());
        sstBuilder.put("6".getBytes(), "6.1".getBytes());
        SortedStringTable sst = sstBuilder.build(0, new LRUCache<>(KB), Engine.getPathOfSST(tempDir, 0));

        assertTrue(sst.numberOfBlock() > 1);

        sst.getFile().close();
    }

    byte[] keyOf(int i) {
        return "key_%03d".formatted(i * 5).getBytes();
    }

    byte[] valueOf(int i) {
        return "value_%010d".formatted(i).getBytes();
    }

    SortedStringTable generateSortedStringTable() throws IOException {
        SortedStringTable.SortedStringTableBuilder sstBuilder = new SortedStringTable.SortedStringTableBuilder(128);
        for (int i = 0; i < 100; i++) {
            sstBuilder.put(keyOf(i), valueOf(i));
        }
        return sstBuilder.build(0, new LRUCache<>(KB), Engine.getPathOfSST(tempDir, 0));
    }

    @Test
    void testEncodeAndDecodeSortedStringTable() throws IOException {
        SortedStringTable sst = generateSortedStringTable();
        SortedStringTable open = SortedStringTable.open(0, new LRUCache<>(KB), SortedStringTable.FileObject.open(Engine.getPathOfSST(tempDir, 0)));
        assertEquals(sst.numberOfBlock(), open.numberOfBlock());

        assertEquals(sst.getMetaBlocks(), open.getMetaBlocks());

        assertArrayEquals(sst.getFirstKey(), open.getFirstKey());
        assertArrayEquals(sst.getLastKey(), open.getLastKey());

        open.getFile().close();
        sst.getFile().close();
    }

    @Test
    void testSortedStringTableIteratorSeekToFirst() throws IOException {
        SortedStringTable sst = generateSortedStringTable();
        SortedStringTable.SortedStringTableIterator iter = SortedStringTable.SortedStringTableIterator.createAndSeekToFirst(sst);
        for (int i = 0; i < 5; i++) {
            for (int j = 0; j < 100; j++) {
                assertTrue(iter.isValid());
                assertArrayEquals(keyOf(j), iter.key());
                assertArrayEquals(valueOf(j), iter.value());
                iter.next();
            }
            iter.seekToFirst();
        }
        sst.getFile().close();
    }

    @Test
    void testSortedStringTableIteratorSeekToKey() throws IOException {
        SortedStringTable sst = generateSortedStringTable();
        SortedStringTable.SortedStringTableIterator iter = SortedStringTable.SortedStringTableIterator.createAndSeekToKey(sst, keyOf(0));
        for (int offset = 1; offset <= 5; offset++) {
            for (int i = 0; i < 100; i++) {
                assertTrue(iter.isValid());
                assertArrayEquals(keyOf(i), iter.key());
                assertArrayEquals(valueOf(i), iter.value());
                iter.seekToKey("key_%03d".formatted(i * 5 + offset).getBytes());
            }
            iter.seekToKey("k".getBytes());
        }
        sst.getFile().close();
    }
}
