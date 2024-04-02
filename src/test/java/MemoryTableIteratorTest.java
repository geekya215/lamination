import io.geekya215.lamination.Bound;
import io.geekya215.lamination.MemoryTable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static io.geekya215.lamination.Bound.*;
import static org.junit.jupiter.api.Assertions.*;

public class MemoryTableIteratorTest {
    MemoryTable memoryTable = MemoryTable.create(0);

    @BeforeEach
    void initMemoryTable() throws IOException {
        memoryTable.put("key1".getBytes(), "value1".getBytes());
        memoryTable.put("key2".getBytes(), "value2".getBytes());
        memoryTable.put("key3".getBytes(), "value3".getBytes());
        memoryTable.put("key4".getBytes(), "value4".getBytes());
        memoryTable.put("key5".getBytes(), "value5".getBytes());
    }

    @Test
    void testMemoryTableIteratorIncluded2Included() {
        Bound<byte[]> lower = included("key1".getBytes());
        Bound<byte[]> upper = included("key3".getBytes());
        MemoryTable.MemoryTableIterator iter = memoryTable.scan(lower, upper);

        for (int i = 1; i <= 3; i++) {
            assertTrue(iter.isValid());
            assertArrayEquals("value%d".formatted(i).getBytes(), iter.value());
            iter.next();
        }

        assertFalse(iter.isValid());
    }

    @Test
    void testMemoryTableIteratorIncluded2Excluded() {
        Bound<byte[]> lower = included("key1".getBytes());
        Bound<byte[]> upper = excluded("key3".getBytes());
        MemoryTable.MemoryTableIterator iter = memoryTable.scan(lower, upper);

        for (int i = 1; i < 3; i++) {
            assertTrue(iter.isValid());
            assertArrayEquals("value%d".formatted(i).getBytes(), iter.value());
            iter.next();
        }

        assertFalse(iter.isValid());
    }

    @Test
    void testMemoryTableIteratorIncluded2Unbound() {
        Bound<byte[]> lower = included("key1".getBytes());
        Bound<byte[]> upper = unbound();
        MemoryTable.MemoryTableIterator iter = memoryTable.scan(lower, upper);

        for (int i = 1; i < 6; i++) {
            assertTrue(iter.isValid());
            assertArrayEquals("value%d".formatted(i).getBytes(), iter.value());
            iter.next();
        }

        assertFalse(iter.isValid());
    }

    @Test
    void testMemoryTableIteratorExcluded2Included() {
        Bound<byte[]> lower = excluded("key1".getBytes());
        Bound<byte[]> upper = included("key3".getBytes());
        MemoryTable.MemoryTableIterator iter = memoryTable.scan(lower, upper);

        for (int i = 2; i <= 3; i++) {
            assertTrue(iter.isValid());
            assertArrayEquals("value%d".formatted(i).getBytes(), iter.value());
            iter.next();
        }

        assertFalse(iter.isValid());
    }

    @Test
    void testMemoryTableIteratorExcluded2Excluded() {
        Bound<byte[]> lower = excluded("key1".getBytes());
        Bound<byte[]> upper = excluded("key3".getBytes());
        MemoryTable.MemoryTableIterator iter = memoryTable.scan(lower, upper);

        for (int i = 2; i < 3; i++) {
            assertTrue(iter.isValid());
            assertArrayEquals("value%d".formatted(i).getBytes(), iter.value());
            iter.next();
        }

        assertFalse(iter.isValid());
    }

    @Test
    void testMemoryTableIteratorExcluded2Unbound() {
        Bound<byte[]> lower = excluded("key1".getBytes());
        Bound<byte[]> upper = unbound();
        MemoryTable.MemoryTableIterator iter = memoryTable.scan(lower, upper);

        for (int i = 2; i < 6; i++) {
            assertTrue(iter.isValid());
            assertArrayEquals("value%d".formatted(i).getBytes(), iter.value());
            iter.next();
        }

        assertFalse(iter.isValid());
    }

    @Test
    void testMemoryTableIteratorUnbound2Included() {
        Bound<byte[]> lower = unbound();
        Bound<byte[]> upper = included("key3".getBytes());
        MemoryTable.MemoryTableIterator iter = memoryTable.scan(lower, upper);

        for (int i = 1; i <= 3; i++) {
            assertTrue(iter.isValid());
            assertArrayEquals("value%d".formatted(i).getBytes(), iter.value());
            iter.next();
        }

        assertFalse(iter.isValid());
    }

    @Test
    void testMemoryTableIteratorUnbound2Excluded() {
        Bound<byte[]> lower = unbound();
        Bound<byte[]> upper = excluded("key3".getBytes());
        MemoryTable.MemoryTableIterator iter = memoryTable.scan(lower, upper);

        for (int i = 1; i < 3; i++) {
            assertTrue(iter.isValid());
            assertArrayEquals("value%d".formatted(i).getBytes(), iter.value());
            iter.next();
        }

        assertFalse(iter.isValid());
    }

    @Test
    void testMemoryTableIteratorUnbound2Unbound() {
        Bound<byte[]> lower = unbound();
        Bound<byte[]> upper = unbound();
        MemoryTable.MemoryTableIterator iter = memoryTable.scan(lower, upper);

        for (int i = 1; i < 6; i++) {
            assertTrue(iter.isValid());
            assertArrayEquals("value%d".formatted(i).getBytes(), iter.value());
            iter.next();
        }

        assertFalse(iter.isValid());
    }

    @Test
    void testMemoryTableIteratorOutOfRange() {
        Bound<byte[]> lower = included("key9".getBytes());
        Bound<byte[]> upper = unbound();
        MemoryTable.MemoryTableIterator iter = memoryTable.scan(lower, upper);

        assertFalse(iter.isValid());
    }

    @Test
    void testMemoryTableIteratorInEmptyMemoryTable() {
        MemoryTable emptyMemoryTable = MemoryTable.create(0);
        Bound<byte[]> lower = unbound();
        Bound<byte[]> upper = unbound();
        MemoryTable.MemoryTableIterator iter = emptyMemoryTable.scan(lower, upper);
        assertFalse(iter.isValid());
    }
}
