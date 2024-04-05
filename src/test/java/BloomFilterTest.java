import io.geekya215.lamination.BloomFilter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class BloomFilterTest {
    BloomFilter bloomFilter;

    @BeforeEach
    void setup() {
        bloomFilter = new BloomFilter(5);
        bloomFilter.add("tom".getBytes());
        bloomFilter.add("jack".getBytes());
        bloomFilter.add("alice".getBytes());
        bloomFilter.add("bob".getBytes());
        bloomFilter.add("john".getBytes());
    }

    @Test
    void testContainElement() {
        assertTrue(bloomFilter.contain("tom".getBytes()));
        assertFalse(bloomFilter.contain("peter".getBytes()));
    }

    @Test
    void testBloomFilterDecode() {
        byte[] encode = bloomFilter.encode();
        BloomFilter decode = BloomFilter.decode(encode);
        assertTrue(decode.contain("tom".getBytes()));
        assertFalse(decode.contain("peter".getBytes()));
    }
}
