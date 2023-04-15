package io.geekya215.lamination;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class Bytes implements Comparable<Bytes> {
    private final byte[] values;

    public Bytes(byte[] bytes) {
        Objects.requireNonNull(bytes);
        this.values = bytes;
    }

    public static Bytes of(byte[] bytes) {
        return new Bytes(bytes);
    }

    public static Bytes of(int... ints) {
        byte[] bytes = new byte[ints.length];
        for (int i = 0; i < ints.length; i++) {
            bytes[i] = (byte) ints[i];
        }
        return of(bytes);
    }

    public static Bytes of(List<Byte> bytes) {
        byte[] bs = new byte[bytes.size()];
        for (int i = 0; i < bytes.size(); i++) {
            bs[i] = bytes.get(i);
        }
        return of(bs);
    }

    public byte[] values() {
        return values;
    }

    public int length() {
        return values.length;
    }

    public Bytes slice(int start, int end) {
        Objects.checkFromToIndex(start, end, length());
        byte[] newBytes = new byte[end - start];
        System.arraycopy(values, start, newBytes, 0, newBytes.length);
        return new Bytes(newBytes);
    }

    @Override
    public int compareTo(Bytes o) {
        return Arrays.compare(values, o.values);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Bytes bytes = (Bytes) o;

        return Arrays.equals(values, bytes.values);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(values);
    }
}
