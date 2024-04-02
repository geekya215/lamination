import io.geekya215.lamination.MergeIterator;
import io.geekya215.lamination.tuple.Tuple2;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class MergeIteratorTest {
    Tuple2<byte[], byte[]> getBytesTuple2(String key, String value) {
        return Tuple2.of(key.getBytes(), value.getBytes());
    }

    void checkResult(List<Tuple2<byte[], byte[]>> expected, MergeIterator iter) {
        for (Tuple2<byte[], byte[]> entry : expected) {
            assertTrue(iter.isValid());
            assertArrayEquals(entry.t1(), iter.key());
            assertArrayEquals(entry.t2(), iter.value());
            iter.next();
        }
        assertFalse(iter.isValid());
    }

    @Test
    void testMergeIteratorOverwrite() {
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

        MockIterator iter3 = new MockIterator(List.of(
                getBytesTuple2("b", "2.3"),
                getBytesTuple2("c", "3.3"),
                getBytesTuple2("d", "4.3")
        ));

        MergeIterator mergeIter = MergeIterator.create(List.of(iter1, iter2, iter3));

        checkResult(List.of(
                getBytesTuple2("a", "1.2"),
                getBytesTuple2("b", "2.3"),
                getBytesTuple2("c", "3.3"),
                getBytesTuple2("d", "4.3")
        ), mergeIter);
    }

    @Test
    void testMergeIteratorWithAllInvalid() {
        MockIterator iter1 = new MockIterator(List.of());
        MockIterator iter2 = new MockIterator(List.of());
        MockIterator iter3 = new MockIterator(List.of());

        MergeIterator mergeIterator = MergeIterator.create(List.of(iter1, iter2, iter3));

        assertFalse(mergeIterator.isValid());
    }

    @Test
    void testMergeIteratorWithSomeInvalid() {
        MockIterator iter1 = new MockIterator(List.of());
        MockIterator iter2 = new MockIterator(List.of(
                getBytesTuple2("a", "1.1"),
                getBytesTuple2("b", "1.2"),
                getBytesTuple2("c", "1.3")
        ));
        MockIterator iter3 = new MockIterator(List.of());

        MergeIterator mergeIterator = MergeIterator.create(List.of(iter1, iter2, iter3));

        checkResult(List.of(
                getBytesTuple2("a", "1.1"),
                getBytesTuple2("b", "1.2"),
                getBytesTuple2("c", "1.3")
        ), mergeIterator);
    }


    @Test
    void testMergeIteratorNormal() {
        MockIterator iter1 = new MockIterator(List.of(
                getBytesTuple2("a", "1.1"),
                getBytesTuple2("b", "1.2"),
                getBytesTuple2("c", "1.3")
        ));

        MockIterator iter2 = new MockIterator(List.of(
                getBytesTuple2("d", "1.4"),
                getBytesTuple2("e", "1.5"),
                getBytesTuple2("f", "1.6")
        ));

        MockIterator iter3 = new MockIterator(List.of(
                getBytesTuple2("g", "1.7"),
                getBytesTuple2("h", "1.8"),
                getBytesTuple2("i", "1.9")
        ));
        MockIterator iter4 = new MockIterator(List.of(
                getBytesTuple2("j", "2.1"),
                getBytesTuple2("k", "2.2"),
                getBytesTuple2("l", "2.3")
        ));


        List<Tuple2<byte[], byte[]>> expected = List.of(
                getBytesTuple2("a", "1.1"),
                getBytesTuple2("b", "1.2"),
                getBytesTuple2("c", "1.3"),
                getBytesTuple2("d", "1.4"),
                getBytesTuple2("e", "1.5"),
                getBytesTuple2("f", "1.6"),
                getBytesTuple2("g", "1.7"),
                getBytesTuple2("h", "1.8"),
                getBytesTuple2("i", "1.9"),
                getBytesTuple2("j", "2.1"),
                getBytesTuple2("k", "2.2"),
                getBytesTuple2("l", "2.3")
        );

        MergeIterator mergeIterator2 = MergeIterator.create(List.of(iter4, iter2, iter3, iter1));
        checkResult(expected, mergeIterator2);
    }
}
