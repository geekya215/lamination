package iterators;

import io.geekya215.lamination.iterators.StorageIterator;
import io.geekya215.lamination.iterators.TwoMergeIterator;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class TwoMergeIteratorTest {
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

        TwoMergeIterator<MockIterator, MockIterator> iter = TwoMergeIterator.create(i1, i2);

        checkIterResult(iter, List.of(
            generatePair("a", "1.1"),
            generatePair("b", "2.1"),
            generatePair("c", "3.1"),
            generatePair("d", "4.2")
        ));
    }

    @Test
    void testMerge2() throws IOException {
        MockIterator i2 = new MockIterator(List.of(
            generatePair("a", "1.1"),
            generatePair("b", "2.1"),
            generatePair("c", "3.1")
        ));

        MockIterator i1 = new MockIterator(List.of(
            generatePair("a", "1.2"),
            generatePair("b", "2.2"),
            generatePair("c", "3.2"),
            generatePair("d", "4.2")
        ));

        TwoMergeIterator<MockIterator, MockIterator> iter = TwoMergeIterator.create(i1, i2);

        checkIterResult(iter, List.of(
            generatePair("a", "1.2"),
            generatePair("b", "2.2"),
            generatePair("c", "3.2"),
            generatePair("d", "4.2")
        ));
    }

    @Test
    void testMerge3() throws IOException {
        MockIterator i2 = new MockIterator(List.of(
            generatePair("a", "1.1"),
            generatePair("b", "2.1"),
            generatePair("c", "3.1")
        ));

        MockIterator i1 = new MockIterator(List.of(
            generatePair("b", "2.2"),
            generatePair("c", "3.2"),
            generatePair("d", "4.2")
        ));

        TwoMergeIterator<MockIterator, MockIterator> iter = TwoMergeIterator.create(i1, i2);

        checkIterResult(iter, List.of(
            generatePair("a", "1.1"),
            generatePair("b", "2.2"),
            generatePair("c", "3.2"),
            generatePair("d", "4.2")
        ));
    }

    @Test
    void testMerge4() throws IOException {
        MockIterator i1 = new MockIterator(List.of());

        MockIterator i2 = new MockIterator(List.of(
            generatePair("b", "2.2"),
            generatePair("c", "3.2"),
            generatePair("d", "4.2")
        ));

        TwoMergeIterator<MockIterator, MockIterator> iter = TwoMergeIterator.create(i1, i2);

        checkIterResult(iter, List.of(
            generatePair("b", "2.2"),
            generatePair("c", "3.2"),
            generatePair("d", "4.2")
        ));
    }

    @Test
    void testMerge5() throws IOException {
        MockIterator i2 = new MockIterator(List.of());

        MockIterator i1 = new MockIterator(List.of(
            generatePair("b", "2.2"),
            generatePair("c", "3.2"),
            generatePair("d", "4.2")
        ));

        TwoMergeIterator<MockIterator, MockIterator> iter = TwoMergeIterator.create(i1, i2);

        checkIterResult(iter, List.of(
            generatePair("b", "2.2"),
            generatePair("c", "3.2"),
            generatePair("d", "4.2")
        ));
    }

    @Test
    void testMerge6() throws IOException {
        MockIterator i1 = new MockIterator(List.of());
        MockIterator i2 = new MockIterator(List.of());

        TwoMergeIterator<MockIterator, MockIterator> iter = TwoMergeIterator.create(i1, i2);

        checkIterResult(iter, List.of());
    }
}
