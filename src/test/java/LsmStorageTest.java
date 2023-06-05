import io.geekya215.lamination.Engine;
import io.geekya215.lamination.LsmStorage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class LsmStorageTest {
    @TempDir
    Path tmpDir;

    LsmStorage storage;

    @BeforeEach
    void setup() {
        storage = LsmStorage.open(tmpDir);
    }

    @Test
    void testStorageGet() throws IOException {
        storage.put("1".getBytes(), "233".getBytes());
        storage.put("2".getBytes(), "2333".getBytes());
        storage.put("3".getBytes(), "23333".getBytes());
        assertArrayEquals("233".getBytes(), storage.get("1".getBytes()));
        assertArrayEquals("2333".getBytes(), storage.get("2".getBytes()));
        assertArrayEquals("23333".getBytes(), storage.get("3".getBytes()));
        storage.delete("2".getBytes());
        assertNull(storage.get("2".getBytes()));
    }

    @Test
    void testStorageGetAfterSync() throws IOException {
        storage.put("1".getBytes(), "233".getBytes());
        storage.put("2".getBytes(), "2333".getBytes());
        storage.sync();
        storage.put("3".getBytes(), "23333".getBytes());
        assertArrayEquals("233".getBytes(), storage.get("1".getBytes()));
        assertArrayEquals("2333".getBytes(), storage.get("2".getBytes()));
        assertArrayEquals("23333".getBytes(), storage.get("3".getBytes()));
        storage.delete("2".getBytes());
        assertNull(storage.get("2".getBytes()));
    }
}
