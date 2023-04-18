import io.geekya215.lamination.Bytes;
import io.geekya215.lamination.SsTable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

public class SstTest {
    Path dir;

    @TempDir
    Path tempDir;

    @BeforeEach
    void setUp() {
        dir = tempDir.resolve("1.sst");
    }

    byte[] keyOf(int i) {
        return String.format("key_%03d", i * 5).getBytes();
    }

    byte[] valueOf(int i) {
        return String.format("value_%010d", i).getBytes();
    }

    @Test
    void testBuildSstWithSingleKey() {
        SsTable.SsTableBuilder builder = new SsTable.SsTableBuilder(16);
        builder.put(Bytes.of("233".getBytes()), Bytes.of("233333".getBytes()));
        SsTable sst = builder.build(0);
        sst.persist(dir);
    }

    @Test
    void testBuildSstWithTwoBlocks() {
        SsTable.SsTableBuilder builder = new SsTable.SsTableBuilder(16);
        builder.put(Bytes.of("11".getBytes()), Bytes.of("11".getBytes()));
        builder.put(Bytes.of("22".getBytes()), Bytes.of("22".getBytes()));
        builder.put(Bytes.of("33".getBytes()), Bytes.of("33".getBytes()));
        builder.put(Bytes.of("44".getBytes()), Bytes.of("44".getBytes()));
        builder.put(Bytes.of("55".getBytes()), Bytes.of("55".getBytes()));
        builder.put(Bytes.of("66".getBytes()), Bytes.of("66".getBytes()));
        SsTable sst = builder.build(0);
        sst.persist(dir);
    }

    @Test
    void generateSst() {
        SsTable.SsTableBuilder builder = new SsTable.SsTableBuilder(128);
        for (int i = 0; i < 100; i++) {
            builder.put(Bytes.of(keyOf(i)), Bytes.of(valueOf(i)));
        }
        SsTable sst = builder.build(0);
        sst.persist(dir);
    }
}
