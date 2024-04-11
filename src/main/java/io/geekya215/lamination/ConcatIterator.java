package io.geekya215.lamination;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static io.geekya215.lamination.Constants.EMPTY_BYTE_ARRAY;

public final class ConcatIterator implements StorageIterator {
    static final int INVALID_NEXT_SST_ID = -1;
    private @Nullable SortedStringTable.SortedStringTableIterator iter;
    private final @NotNull List<SortedStringTable> ssts;
    private int nextSSTId;

    public ConcatIterator(SortedStringTable.@Nullable SortedStringTableIterator iter, @NotNull List<SortedStringTable> ssts, int nextSSTId) {
        this.iter = iter;
        this.ssts = ssts;
        this.nextSSTId = nextSSTId;
    }

    public static @NotNull ConcatIterator createAndSeekToFirst(@NotNull List<SortedStringTable> ssts) throws IOException {
        boolean valid =  checkSSTValidation(ssts);
        if (!valid) {
            return new ConcatIterator(null, ssts, INVALID_NEXT_SST_ID);
        }

        if (ssts.isEmpty()) {
            return new ConcatIterator(null, ssts, 0);
        }

        ConcatIterator iter = new ConcatIterator(SortedStringTable.SortedStringTableIterator.createAndSeekToFirst(ssts.getFirst()), ssts, 1);
        iter.skipInvalid();
        return iter;
    }

    public static @NotNull ConcatIterator createAndSeekToKey(@NotNull List<SortedStringTable> ssts, byte @NotNull [] key) throws IOException {
        boolean valid = checkSSTValidation(ssts);
        if (!valid) {
            return new ConcatIterator(null, ssts, INVALID_NEXT_SST_ID);
        }

        int index = 0;
        for (; index < ssts.size(); index++) {
            SortedStringTable sst = ssts.get(index);
            if (Arrays.compare(sst.getFirstKey(), key) <= 0) {
                break;
            }
        }
        index = index == 0 ? 0 : index - 1;
        if (index >= ssts.size()) {
            return new ConcatIterator(null, ssts, ssts.size());
        }
        ConcatIterator iter = new ConcatIterator(SortedStringTable.SortedStringTableIterator.createAndSeekToKey(ssts.get(index), key), ssts, index + 1);
        iter.skipInvalid();
        return iter;
    }

    static boolean checkSSTValidation(@NotNull List<SortedStringTable> ssts) {
        // for single sst
        // first key <= last key
        for (SortedStringTable sst : ssts) {
            if (Arrays.compare(sst.getFirstKey(), sst.getLastKey()) > 0) {
                return false;
            }
        }

        // for any two adjacent sst
        //  sst #1       sst #2
        // last key  <  first key
        for (int i = 0; i < ssts.size() - 1; i++) {
            if (Arrays.compare(ssts.get(i).getLastKey(), ssts.get(i + 1).getFirstKey()) >= 0) {
                return false;
            }
        }

        return true;
    }

    void skipInvalid() throws IOException {
        while (iter != null) {
            if (iter.isValid()) {
                break;
            }
            if (nextSSTId >= ssts.size()) {
                iter = null;
            } else {
                iter = SortedStringTable.SortedStringTableIterator.createAndSeekToFirst(ssts.get(nextSSTId));
                nextSSTId += 1;
            }
        }
    }

    @Override
    public byte @NotNull [] key() {
        return iter != null ? iter.key() : EMPTY_BYTE_ARRAY;
    }

    @Override
    public byte @NotNull [] value() {
        return iter != null ? iter.value() : EMPTY_BYTE_ARRAY;
    }

    @Override
    public boolean isValid() {
        return iter != null && iter.isValid();
    }

    @Override
    public void next() throws IOException {
        if (iter != null) {
            iter.next();
        }
        skipInvalid();
    }
}
