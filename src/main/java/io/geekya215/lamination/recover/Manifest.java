package io.geekya215.lamination.recover;

import io.geekya215.lamination.compact.CompactionTask;
import io.geekya215.lamination.tuple.Tuple2;
import org.jetbrains.annotations.NotNull;

import java.io.*;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

public final class Manifest implements Closeable {
    private final @NotNull DataOutputStream dos;
    private final @NotNull ReentrantLock lock;

    public Manifest(@NotNull File file, boolean append) throws FileNotFoundException {
        this.dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(file, append)));
        this.lock = new ReentrantLock();
    }

    public static @NotNull Manifest create(@NotNull Path path) throws IOException {
        return new Manifest(path.toFile(), false);
    }

    public static @NotNull Tuple2<Manifest, List<Track>> recover(@NotNull Path path) throws IOException {
        File file = path.toFile();
        final List<Track> tracks = new ArrayList<>();
        try (FileInputStream fis = new FileInputStream(file);
             DataInputStream dis = new DataInputStream(fis)
        ) {
            while (dis.available() > 0) {
                byte type = dis.readByte();
                // Todo
                // use human readable alias instead of integer literal
                switch (type) {
                    case 0 -> {
                        int id = dis.readInt();
                        tracks.add(new Track.Flush(id));
                    }
                    case 1 -> {
                        int id = dis.readInt();
                        tracks.add(new Track.Create(id));
                    }
                    case 2 -> {
                        int taskLength = dis.readInt();
                        byte[] taskBuf = dis.readNBytes(taskLength);
                        CompactionTask.SimpleTask simpleTask = CompactionTask.SimpleTask.decode(taskBuf);
                        int outputsLength = dis.readInt();
                        List<Integer> outputs = new ArrayList<>(outputsLength);
                        for (int i = 0; i < outputsLength; i++) {
                            outputs.add(dis.readInt());
                        }
                        tracks.add(new Track.Compact(simpleTask, outputs));
                    }
                    default -> throw new IllegalArgumentException("unsupported track type");
                }
            }
        }
        return Tuple2.of(new Manifest(file, true), tracks);
    }

    public void addTrack(@NotNull Track track) throws IOException {
        lock.lock();
        try {
            switch (track) {
                case Track.Flush _ -> dos.writeByte(0);
                case Track.Create _ -> dos.writeByte(1);
                case Track.Compact _ -> dos.writeByte(2);
            }
            byte[] buf = track.encode();
            dos.write(buf);
            dos.flush();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() throws IOException {
        dos.close();
    }
}
