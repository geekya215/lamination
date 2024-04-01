import io.geekya215.lamination.MemoryTable;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

public class MemoryTableTest {
    @Test
    void testMemoryTableGet() throws IOException {
        MemoryTable memoryTable = MemoryTable.create(0);
        memoryTable.put("key1".getBytes(), "value1".getBytes());
        memoryTable.put("key2".getBytes(), "value2".getBytes());
        memoryTable.put("key3".getBytes(), "value3".getBytes());
        assertArrayEquals("value1".getBytes(), memoryTable.get("key1".getBytes()));
        assertArrayEquals("value2".getBytes(), memoryTable.get("key2".getBytes()));
        assertArrayEquals("value3".getBytes(), memoryTable.get("key3".getBytes()));
    }

    @Test
    void testMemoryTableOverwrite() throws IOException {
        MemoryTable memoryTable = MemoryTable.create(0);
        memoryTable.put("key1".getBytes(), "value1".getBytes());
        memoryTable.put("key2".getBytes(), "value2".getBytes());
        memoryTable.put("key3".getBytes(), "value3".getBytes());
        memoryTable.put("key1".getBytes(), "value11".getBytes());
        memoryTable.put("key2".getBytes(), "value22".getBytes());
        memoryTable.put("key3".getBytes(), "value33".getBytes());
        assertArrayEquals("value11".getBytes(), memoryTable.get("key1".getBytes()));
        assertArrayEquals("value22".getBytes(), memoryTable.get("key2".getBytes()));
        assertArrayEquals("value33".getBytes(), memoryTable.get("key3".getBytes()));
    }
}
