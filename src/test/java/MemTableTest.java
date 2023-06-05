import io.geekya215.lamination.Block;
import io.geekya215.lamination.LRUCache;
import io.geekya215.lamination.MemTable;
import io.geekya215.lamination.SSTable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.*;

public class MemTableTest {
    LRUCache<Long, Block> blockCache;

    @BeforeEach
    void setup() {
        blockCache = new LRUCache<>();
    }

    ByteBuffer byteWrap(String s) {
        return ByteBuffer.wrap(s.getBytes());
    }

    @Test
    void testMemTableGet() {
        MemTable memTable = MemTable.create();
        memTable.put(byteWrap("key1"), byteWrap("value1"));
        memTable.put(byteWrap("key2"), byteWrap("value2"));
        memTable.put(byteWrap("key3"), byteWrap("value3"));
        assertEquals(byteWrap("value1"), memTable.get(byteWrap("key1")));
        assertEquals(byteWrap("value2"), memTable.get(byteWrap("key2")));
        assertEquals(byteWrap("value3"), memTable.get(byteWrap("key3")));
    }

    @Test
    void testMemTableOverwrite() {
        MemTable memTable = MemTable.create();
        memTable.put(byteWrap("key1"), byteWrap("value1"));
        memTable.put(byteWrap("key2"), byteWrap("value2"));
        memTable.put(byteWrap("key3"), byteWrap("value3"));
        memTable.put(byteWrap("key1"), byteWrap("value11"));
        memTable.put(byteWrap("key2"), byteWrap("value22"));
        memTable.put(byteWrap("key3"), byteWrap("value33"));
        assertEquals(byteWrap("value11"), memTable.get(byteWrap("key1")));
        assertEquals(byteWrap("value22"), memTable.get(byteWrap("key2")));
        assertEquals(byteWrap("value33"), memTable.get(byteWrap("key3")));
    }

    @Test
    void testMemTableFlush(@TempDir File tempDir) throws IOException {
        MemTable memTable = MemTable.create();
        memTable.put(byteWrap("key1"), byteWrap("value1"));
        memTable.put(byteWrap("key2"), byteWrap("value2"));
        memTable.put(byteWrap("key3"), byteWrap("value3"));

        SSTable.SSTableBuilder builder = new SSTable.SSTableBuilder(128);
        memTable.flush(builder);
        SSTable sst = builder.build(0, tempDir, blockCache);

        SSTable open = SSTable.open(0, tempDir, blockCache);

        assertEquals(sst.getMetaBlocks(), open.getMetaBlocks());
        assertEquals(sst.getMetaBlockOffset(), open.getMetaBlockOffset());
    }

    @Test
    void testMemTableIterator() throws IOException {
        MemTable memTable = MemTable.create();
        memTable.put(byteWrap("key1"), byteWrap("value1"));
        memTable.put(byteWrap("key2"), byteWrap("value2"));
        memTable.put(byteWrap("key3"), byteWrap("value3"));
        memTable.put(byteWrap("key4"), byteWrap("value4"));

        MemTable.MemTableIterator iterator = memTable.scan(byteWrap("key2"), byteWrap("key3"));

        assertArrayEquals("key2".getBytes(), iterator.key());
        assertArrayEquals("value2".getBytes(), iterator.value());
        iterator.next();

        assertArrayEquals("key3".getBytes(), iterator.key());
        assertArrayEquals("value3".getBytes(), iterator.value());
        iterator.next();

        assertFalse(iterator.isValid());
    }
}
