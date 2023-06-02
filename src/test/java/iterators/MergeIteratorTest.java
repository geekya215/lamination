package iterators;

import io.geekya215.lamination.iterators.MergeIterator;
import io.geekya215.lamination.iterators.StorageIterator;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class MergeIteratorTest {
    Pair<byte[], byte[]> generatePair(String key, String value) {
        return new Pair<>(key.getBytes(), value.getBytes());
    }

    void checkIterResult(StorageIterator iter, List<Pair<byte[], byte[]>> expected) throws IOException {
        for (Pair<byte[], byte[]> pair : expected) {
            assertTrue(iter.isValid());
            assertArrayEquals(pair.fst(), iter.key());
            assertArrayEquals(pair.snd(), iter.value());
            iter.next();
        }

        assertFalse(iter.isValid());
    }

    @Test
    void testMerge1() throws IOException {
        MockIterator i1 = new MockIterator(List.of(
            generatePair("a", "1.1"),
            generatePair("b", "2.1"),
            generatePair("c", "3.1")
        ));

        MockIterator i2 = new MockIterator(List.of(
            generatePair("a", "1.2"),
            generatePair("b", "2.2"),
            generatePair("c", "3.2"),
            generatePair("d", "4.2")
        ));

        MockIterator i3 = new MockIterator(List.of(
            generatePair("b", "2.3"),
            generatePair("c", "3.3"),
            generatePair("d", "4.3")
        ));

        MergeIterator iter = MergeIterator.create(List.of(i1, i2, i3));

        checkIterResult(iter, List.of(
            generatePair("a", "1.1"),
            generatePair("b", "2.1"),
            generatePair("c", "3.1"),
            generatePair("d", "4.2")
        ));
    }

    @Test
    void testMerge2() throws IOException {
        MockIterator i1 = new MockIterator(List.of(
            generatePair("a", "1.1"),
            generatePair("b", "2.1"),
            generatePair("c", "3.1")
        ));

        MockIterator i2 = new MockIterator(List.of(
            generatePair("d", "1.2"),
            generatePair("e", "2.2"),
            generatePair("f", "3.2"),
            generatePair("g", "4.2")
        ));

        MockIterator i3 = new MockIterator(List.of(
            generatePair("h", "1.3"),
            generatePair("i", "2.3"),
            generatePair("j", "3.3"),
            generatePair("k", "4.3")
        ));

        MockIterator i4 = new MockIterator(List.of());

        MergeIterator iter = MergeIterator.create(List.of(i1, i2, i3, i4));

        checkIterResult(iter, List.of(
            generatePair("a", "1.1"),
            generatePair("b", "2.1"),
            generatePair("c", "3.1"),
            generatePair("d", "1.2"),
            generatePair("e", "2.2"),
            generatePair("f", "3.2"),
            generatePair("g", "4.2"),
            generatePair("h", "1.3"),
            generatePair("i", "2.3"),
            generatePair("j", "3.3"),
            generatePair("k", "4.3")
        ));
    }

    @Test
    void testMergeEmpty() throws IOException {
        MergeIterator iter = MergeIterator.create(List.of());
        checkIterResult(iter, List.of());
    }
}
