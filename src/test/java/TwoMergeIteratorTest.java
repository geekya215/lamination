import io.geekya215.lamination.StorageIterator;
import io.geekya215.lamination.TwoMergeIterator;
import io.geekya215.lamination.tuple.Tuple2;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class TwoMergeIteratorTest {
    Tuple2<byte[], byte[]> getBytesTuple2(String key, String value) {
        return Tuple2.of(key.getBytes(), value.getBytes());
    }

    void checkResult(List<Tuple2<byte[], byte[]>> expected, StorageIterator iter) throws IOException {
        for (Tuple2<byte[], byte[]> entry : expected) {
            assertTrue(iter.isValid());
            assertArrayEquals(entry.t1(), iter.key());
            assertArrayEquals(entry.t2(), iter.value());
            iter.next();
        }
        assertFalse(iter.isValid());
    }

    @Test
    void testMergeIteratorPreferIter1() throws IOException {
        MockIterator iter1 = new MockIterator(List.of(
                getBytesTuple2("a", "1.1"),
                getBytesTuple2("b", "2.1"),
                getBytesTuple2("c", "3.1")));

        MockIterator iter2 = new MockIterator(List.of(
                getBytesTuple2("a", "1.2"),
                getBytesTuple2("b", "2.2"),
                getBytesTuple2("c", "3.2"),
                getBytesTuple2("d", "4.2")
        ));

        StorageIterator iter = TwoMergeIterator.create(iter1, iter2);
        checkResult(List.of(
                getBytesTuple2("a", "1.1"),
                getBytesTuple2("b", "2.1"),
                getBytesTuple2("c", "3.1"),
                getBytesTuple2("d", "4.2")
        ), iter);
    }

    @Test
    void testMergeIteratorPreferIter2() throws IOException {
        MockIterator iter1 = new MockIterator(List.of(
                getBytesTuple2("a", "1.1"),
                getBytesTuple2("b", "2.1"),
                getBytesTuple2("c", "3.1")));

        MockIterator iter2 = new MockIterator(List.of(
                getBytesTuple2("a", "1.2"),
                getBytesTuple2("b", "2.2"),
                getBytesTuple2("c", "3.2"),
                getBytesTuple2("d", "4.2")
        ));

        StorageIterator iter = TwoMergeIterator.create(iter2, iter1);
        checkResult(List.of(
                getBytesTuple2("a", "1.2"),
                getBytesTuple2("b", "2.2"),
                getBytesTuple2("c", "3.2"),
                getBytesTuple2("d", "4.2")
        ), iter);
    }

    @Test
    void testMergeIteratorIter1IsEmpty() throws IOException {
        MockIterator iter1 = new MockIterator(List.of());

        MockIterator iter2 = new MockIterator(List.of(
                getBytesTuple2("a", "1.2"),
                getBytesTuple2("b", "2.2"),
                getBytesTuple2("c", "3.2"),
                getBytesTuple2("d", "4.2")
        ));

        StorageIterator iter = TwoMergeIterator.create(iter1, iter2);
        checkResult(List.of(
                getBytesTuple2("a", "1.2"),
                getBytesTuple2("b", "2.2"),
                getBytesTuple2("c", "3.2"),
                getBytesTuple2("d", "4.2")
        ), iter);
    }

    @Test
    void testMergeIteratorIter2IsEmpty() throws IOException {
        MockIterator iter1 = new MockIterator(List.of(
                getBytesTuple2("a", "1.2"),
                getBytesTuple2("b", "2.2"),
                getBytesTuple2("c", "3.2"),
                getBytesTuple2("d", "4.2")
        ));

        MockIterator iter2 = new MockIterator(List.of());

        StorageIterator iter = TwoMergeIterator.create(iter1, iter2);
        checkResult(List.of(
                getBytesTuple2("a", "1.2"),
                getBytesTuple2("b", "2.2"),
                getBytesTuple2("c", "3.2"),
                getBytesTuple2("d", "4.2")
        ), iter);
    }

    @Test
    void testMergeIteratorWithEmptyIter() throws IOException {
        MockIterator iter1 = new MockIterator(List.of());
        MockIterator iter2 = new MockIterator(List.of());

        StorageIterator iter = TwoMergeIterator.create(iter1, iter2);
        checkResult(List.of(), iter);
    }
}
