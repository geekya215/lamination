package io.geekya215.lamination.compact;

import io.geekya215.lamination.Constants;
import io.geekya215.lamination.Encoder;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.List;

import static io.geekya215.lamination.Constants.*;

public sealed interface CompactionTask extends Encoder
        permits CompactionTask.SimpleTask, CompactionTask.LeveledTask, CompactionTask.TieredTask, CompactionTask.FullTask {
    record SimpleTask(
            int upperLevel,
            @NotNull List<Integer> upperLevelSSTIds,
            int lowerLevel,
            @NotNull List<Integer> lowerLevelSSTIds,
            boolean isLowerLevelBottomLevel) implements CompactionTask {
        @Override
        public byte @NotNull [] encode() {
            // we assume max level <= 256
            int upperLevelSSTIdsSize = upperLevelSSTIds.size();
            int lowerLevelSSTIdsSize = lowerLevelSSTIds.size();
            final byte[] buf = new byte[SIZE_OF_U16 + SIZE_OF_U32 + upperLevelSSTIdsSize * SIZE_OF_U32 + SIZE_OF_U16 + SIZE_OF_U32 + lowerLevelSSTIdsSize * SIZE_OF_U32 + SIZE_OF_U8];
            int cursor = 0;
            buf[cursor] = (byte) (upperLevel >> 8);
            buf[cursor + 1] = (byte) upperLevel;
            cursor += 2;

            buf[cursor] = (byte) (upperLevelSSTIdsSize >> 24);
            buf[cursor + 1] = (byte) (upperLevelSSTIdsSize >> 16);
            buf[cursor + 2] = (byte) (upperLevelSSTIdsSize >> 8);
            buf[cursor + 3] = (byte) upperLevelSSTIdsSize;
            cursor += 4;

            for (int upperLevelSSTId : upperLevelSSTIds) {
                buf[cursor] = (byte) (upperLevelSSTId >> 24);
                buf[cursor + 1] = (byte) (upperLevelSSTId >> 16);
                buf[cursor + 2] = (byte) (upperLevelSSTId >> 8);
                buf[cursor + 3] = (byte) upperLevelSSTId;
                cursor += 4;
            }

            buf[cursor] = (byte) (lowerLevel >> 8);
            buf[cursor + 1] = (byte) lowerLevel;
            cursor += 2;

            buf[cursor] = (byte) (lowerLevelSSTIdsSize >> 24);
            buf[cursor + 1] = (byte) (lowerLevelSSTIdsSize >> 16);
            buf[cursor + 2] = (byte) (lowerLevelSSTIdsSize >> 8);
            buf[cursor + 3] = (byte) lowerLevelSSTIdsSize;
            cursor += 4;

            for (int lowerLevelSSTId : lowerLevelSSTIds) {
                buf[cursor] = (byte) (lowerLevelSSTId >> 24);
                buf[cursor + 1] = (byte) (lowerLevelSSTId >> 16);
                buf[cursor + 2] = (byte) (lowerLevelSSTId >> 8);
                buf[cursor + 3] = (byte) lowerLevelSSTId;
                cursor += 4;
            }

            if (isLowerLevelBottomLevel) {
                buf[cursor] = 1;
            }

            return buf;
        }

        public static @NotNull SimpleTask decode(byte @NotNull [] buf) {
            int cursor = 0;

            int upperLevel = (buf[cursor] & 0xFF) << 8 | (buf[cursor + 1] & 0xFF);
            cursor += 2;

            int upperLevelSSTIdsSize = (buf[cursor] & 0xFF) << 24 | (buf[cursor + 1] & 0xFF) << 16 |
                    (buf[cursor + 2] & 0xFF) << 8 | (buf[cursor + 3] & 0xFF);
            cursor += 4;

            List<Integer> upperLevelSSTIds = new ArrayList<>(upperLevelSSTIdsSize);
            for (int i = 0; i < upperLevelSSTIdsSize; i++) {
                int upperLevelSSTId = (buf[cursor] & 0xFF) << 24 | (buf[cursor + 1] & 0xFF) << 16 |
                        (buf[cursor + 2] & 0xFF) << 8 | (buf[cursor + 3] & 0xFF);
                upperLevelSSTIds.add(upperLevelSSTId);
                cursor += 4;
            }

            int lowerLevel = (buf[cursor] & 0xFF) << 8 | (buf[cursor + 1] & 0xFF);
            cursor += 2;

            int lowerLevelSSTIdSize = (buf[cursor] & 0xFF) << 24 | (buf[cursor + 1] & 0xFF) << 16 |
                    (buf[cursor + 2] & 0xFF) << 8 | (buf[cursor + 3] & 0xFF);
            cursor += 4;

            List<Integer> lowerLevelSSTIds = new ArrayList<>(lowerLevelSSTIdSize);
            for (int i = 0; i < lowerLevelSSTIdSize; i++) {
                int lowerLevelSSTId = (buf[cursor] & 0xFF) << 24 | (buf[cursor + 1] & 0xFF) << 16 |
                        (buf[cursor + 2] & 0xFF) << 8 | (buf[cursor + 3] & 0xFF);
                lowerLevelSSTIds.add(lowerLevelSSTId);
                cursor += 4;
            }

            boolean isLowerLevelBottomLevel = (buf[cursor] & 1) == 1;

            return new SimpleTask(upperLevel, upperLevelSSTIds, lowerLevel, lowerLevelSSTIds, isLowerLevelBottomLevel);
        }
    }

    record LeveledTask() implements CompactionTask {
        @Override
        public byte @NotNull [] encode() {
            return new byte[0];
        }
    }

    record TieredTask() implements CompactionTask {
        @Override
        public byte @NotNull [] encode() {
            return new byte[0];
        }
    }

    record FullTask() implements CompactionTask {
        @Override
        public byte @NotNull [] encode() {
            return new byte[0];
        }
    }
}
