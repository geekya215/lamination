package io.geekya215.lamination.recover;

import io.geekya215.lamination.Encoder;
import io.geekya215.lamination.compact.CompactionTask;
import org.jetbrains.annotations.NotNull;

import java.util.List;

import static io.geekya215.lamination.Constants.SIZE_OF_U32;

public sealed interface Track extends Encoder permits Track.Compact, Track.Create, Track.Flush {
    record Flush(int id) implements Track {
        @Override
        public byte @NotNull [] encode() {
            final byte[] buf = new byte[SIZE_OF_U32];
            int cursor = 0;

            buf[cursor] = (byte) (id >> 24);
            buf[cursor + 1] = (byte) (id >> 16);
            buf[cursor + 2] = (byte) (id >> 8);
            buf[cursor + 3] = (byte) id;
            return buf;
        }
    }

    record Create(int id) implements Track {
        @Override
        public byte @NotNull [] encode() {
            final byte[] buf = new byte[SIZE_OF_U32];
            int cursor = 0;

            buf[cursor] = (byte) (id >> 24);
            buf[cursor + 1] = (byte) (id >> 16);
            buf[cursor + 2] = (byte) (id >> 8);
            buf[cursor + 3] = (byte) id;
            return buf;
        }
    }

    record Compact(CompactionTask task, List<Integer> outputs) implements Track {
        @Override
        public byte @NotNull [] encode() {
            final byte[] taskBuf = task.encode();
            final int taskBufLength = taskBuf.length;
            final int outputLength = outputs.size();
            final byte[] buf = new byte[SIZE_OF_U32 + taskBufLength + SIZE_OF_U32  + outputLength * SIZE_OF_U32];

            int cursor = 0;

            buf[cursor] = (byte) (taskBufLength >> 24);
            buf[cursor + 1] = (byte) (taskBufLength >> 16);
            buf[cursor + 2] = (byte) (taskBufLength >> 8);
            buf[cursor + 3] = (byte) taskBufLength;
            cursor += 4;

            System.arraycopy(taskBuf, 0, buf, cursor, taskBufLength);
            cursor += taskBufLength;

            buf[cursor] = (byte) (outputLength >> 24);
            buf[cursor + 1] = (byte) (outputLength >> 16);
            buf[cursor + 2] = (byte) (outputLength >> 8);
            buf[cursor + 3] = (byte) outputLength;
            cursor += 4;

            for (int output : outputs) {
                buf[cursor] = (byte) (output >> 24);
                buf[cursor + 1] = (byte) (output >> 16);
                buf[cursor + 2] = (byte) (output >> 8);
                buf[cursor + 3] = (byte) output;
                cursor += 4;
            }

            return buf;
        }
    }
}
