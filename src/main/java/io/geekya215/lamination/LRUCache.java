package io.geekya215.lamination;

import java.util.HashMap;
import java.util.LinkedList;

public final class LRUCache<K, V extends Measurable> implements Cache<K, V> {
    private final HashMap<K, V> map;
    private final LinkedList<K> list;
    private final int cacheSize;
    private int currentSize;

    public LRUCache() {
        this(Options.DEFAULT_BLOCK_CACHE_SIZE);
    }

    public LRUCache(int cacheSize) {
        this.map = new HashMap<>();
        this.list = new LinkedList<>();
        this.cacheSize = cacheSize;
        this.currentSize = 0;
    }

    @Override
    public void put(K key, V value) {
        int size = value.estimateSize();
        while (currentSize + size > cacheSize) {
            K k = list.removeLast();
            V v = map.remove(k);
            currentSize -= v.estimateSize();
        }
        currentSize += size;
        list.addFirst(key);
        map.put(key, value);
    }

    @Override
    public V get(K key) {
        V value = map.get(key);
        if (value != null) {
            list.remove(key);
            list.addFirst(key);
        }
        return value;
    }
}
