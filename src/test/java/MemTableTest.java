import io.geekya215.lamination.Block;
import io.geekya215.lamination.MemTable;
import io.geekya215.lamination.SSTable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class MemTableTest {
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
    void testMemTableFlush(@TempDir Path tempDir) throws IOException {
        MemTable memTable = MemTable.create();
        memTable.put(byteWrap("key1"), byteWrap("value1"));
        memTable.put(byteWrap("key2"), byteWrap("value2"));
        memTable.put(byteWrap("key3"), byteWrap("value3"));

        SSTable.SSTableBuilder builder = new SSTable.SSTableBuilder(128);
        memTable.flush(builder);
        Path path = tempDir.resolve("1.sst");
        SSTable sst = builder.build(0, path);

        SSTable.SSTableIterator sstIterator = SSTable.SSTableIterator.createAndSeekToFirst(sst);
        Block block = sstIterator.next();
        Block.BlockIterator blockIterator = Block.BlockIterator.createAndSeekToFirst(block);

        Block.Entry e1 = blockIterator.next();
        assertEquals(new Block.Entry("key1".getBytes(), "value1".getBytes()), e1);
        Block.Entry e2 = blockIterator.next();
        assertEquals(new Block.Entry("key2".getBytes(), "value2".getBytes()), e2);
        Block.Entry e3 = blockIterator.next();
        assertEquals(new Block.Entry("key3".getBytes(), "value3".getBytes()), e3);

        assertFalse(sstIterator.hasNext());
        assertFalse(blockIterator.hasNext());
    }

    @Test
    void testMemTableIterator() {
        MemTable memTable = MemTable.create();
        memTable.put(byteWrap("key1"), byteWrap("value1"));
        memTable.put(byteWrap("key2"), byteWrap("value2"));
        memTable.put(byteWrap("key3"), byteWrap("value3"));
        memTable.put(byteWrap("key4"), byteWrap("value4"));

        Iterator<Map.Entry<ByteBuffer, ByteBuffer>> iterator = memTable.scan(byteWrap("key2"), byteWrap("key3"));

        Map.Entry<ByteBuffer, ByteBuffer> e2 = iterator.next();
        assertArrayEquals("key2".getBytes(), e2.getKey().array());
        assertArrayEquals("value2".getBytes(), e2.getValue().array());

        Map.Entry<ByteBuffer, ByteBuffer> e3 = iterator.next();
        assertArrayEquals("key3".getBytes(), e3.getKey().array());
        assertArrayEquals("value3".getBytes(), e3.getValue().array());

        assertFalse(iterator.hasNext());
    }
}