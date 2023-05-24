package io.geekya215.lamination;

public interface Cache<K, V> {
    void put(K key, V value);
    V get(K key);
}
