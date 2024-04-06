import io.geekya215.lamination.Engine;
import io.geekya215.lamination.MemoryTable;
import io.geekya215.lamination.Options;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.Executors;

import static io.geekya215.lamination.Constants.KB;
import static org.junit.jupiter.api.Assertions.*;

public class EngineTest {
    @TempDir
    Path tmpDir;

    @Test
    void testEngineGetPutDelete() throws IOException {
        Engine engine = Engine.open(tmpDir, new Options(2 * KB, 2, 4 * KB, false));

        assertNull(engine.get("0".getBytes()));

        engine.put("1".getBytes(), "1".getBytes());
        engine.put("2".getBytes(), "2".getBytes());
        engine.put("3".getBytes(), "3".getBytes());

        assertArrayEquals("1".getBytes(), engine.get("1".getBytes()));
        assertArrayEquals("2".getBytes(), engine.get("2".getBytes()));
        assertArrayEquals("3".getBytes(), engine.get("3".getBytes()));

        engine.delete("1".getBytes());
        assertNull(engine.get("1".getBytes()));

        engine.delete("0".getBytes());
    }

    @Test
    void testEngineForceFreeze() throws IOException {
        Engine engine = Engine.open(tmpDir, new Options(2 * KB, 2, 4 * KB, false));

        engine.put("1".getBytes(), "1".getBytes());
        engine.put("2".getBytes(), "2".getBytes());
        engine.put("3".getBytes(), "3".getBytes());
        engine.forceFreezeMemoryTable();

        List<MemoryTable> immutableMemoryTables = engine.getStorage().getImmutableMemoryTables();
        assertEquals(1, immutableMemoryTables.size());
        assertEquals(6, immutableMemoryTables.get(0).getApproximateSize());

        engine.put("1".getBytes(), "11".getBytes());
        engine.put("2".getBytes(), "22".getBytes());
        engine.put("3".getBytes(), "33".getBytes());
        engine.forceFreezeMemoryTable();

        assertEquals(2, immutableMemoryTables.size());
        assertEquals(9, immutableMemoryTables.get(1).getApproximateSize());
    }

    @Test
    void testEngineFreezeOnCapacityInSingleThread() throws IOException {
        Engine engine = Engine.open(tmpDir, new Options(2 * KB, 1000, KB, false));
        for (int i = 0; i < 1000; i++) {
            final byte[] buf = String.format("%05d", i).getBytes();
            engine.put(buf, buf);
        }
        List<MemoryTable> immutableMemoryTables = engine.getStorage().getImmutableMemoryTables();
        int numOfImmutableMemoryTable = immutableMemoryTables.size();
        assertTrue(numOfImmutableMemoryTable > 0);

        for (int i = 0; i < 1000; i++) {
            engine.delete("1".getBytes());
        }

        assertTrue(immutableMemoryTables.size() > numOfImmutableMemoryTable);
    }

    @Test
    void testEngineFreezeOnCapacityInMultiThread() {
        Engine engine = Engine.open(tmpDir, new Options(2 * KB, 1000, KB, false));
        for (int i = 0; i < 1000; i++) {
            final byte[] buf = String.format("%05d", i).getBytes();
            Executors.newVirtualThreadPerTaskExecutor().submit(() -> {
                try {
                    engine.put(buf, buf);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }
        List<MemoryTable> immutableMemoryTables = engine.getStorage().getImmutableMemoryTables();
        int numOfImmutableMemoryTable = immutableMemoryTables.size();
        assertTrue(numOfImmutableMemoryTable > 0);
    }

    @Test
    void testEngineGetFromImmutableMemoryTable() throws IOException {
        Engine engine = Engine.open(tmpDir, new Options(2 * KB, 1000, KB, false));
        assertNull(engine.get("0".getBytes()));

        engine.put("1".getBytes(), "1".getBytes());
        engine.put("2".getBytes(), "2".getBytes());
        engine.put("3".getBytes(), "3".getBytes());
        engine.forceFreezeMemoryTable();

        engine.delete("1".getBytes());
        engine.delete("2".getBytes());
        engine.put("3".getBytes(), "3".getBytes());
        engine.put("4".getBytes(), "4".getBytes());
        engine.forceFreezeMemoryTable();

        engine.put("1".getBytes(), "11".getBytes());
        engine.put("3".getBytes(), "33".getBytes());

        assertEquals(2, engine.getStorage().getImmutableMemoryTables().size());

        assertArrayEquals("11".getBytes(), engine.get("1".getBytes()));
        assertNull(engine.get("2".getBytes()));
        assertArrayEquals("33".getBytes(), engine.get("3".getBytes()));
        assertArrayEquals("4".getBytes(), engine.get("4".getBytes()));
    }
}
