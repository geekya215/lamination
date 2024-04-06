package io.geekya215.lamination;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.HashMap;
import java.util.Map;

public final class LRUCache<K, V extends Measurable> implements Cache<K, V> {
    private final @NotNull Map<K, Node<K, V>> cache;
    private final @NotNull Node<K, V> dummy;
    private final int capacity;
    private int size;

    public LRUCache(int capacity) {
        this.cache = new HashMap<>();
        this.dummy = Node.createDummyNode();
        this.capacity = capacity;
        this.size = 0;
    }

    @Override
    public void put(@NotNull K key, @NotNull V value) {
        if (cache.containsKey(key)) {
            Node<K, V> existNode = cache.get(key);
            removeNode(existNode);
            addToFront(existNode);
        } else {
            Node<K, V> newNode = new Node<>(key, value);
            // Fixme
            // when value size greater than capacity will get infinite loop
            while (size + value.estimateSize() > capacity) {
                Node<K, V> tail = dummy.prev;
                evict(tail);
            }
            size += value.estimateSize();
            addToFront(newNode);
            cache.put(key, newNode);
        }
    }

    @Override
    public @Nullable V get(@NotNull K key) {
        if (cache.containsKey(key)) {
            Node<K, V> node = cache.get(key);
            removeNode(node);
            addToFront(node);
            return node.value;
        } else {
            return null;
        }
    }

    private void evict(@NotNull Node<K, V> node) {
        removeNode(node);
        cache.remove(node.key);
        size -= node.value.estimateSize();
    }

    private void addToFront(@NotNull Node<K, V> node) {
        node.prev = dummy;
        node.next = dummy.next;
        node.next.prev = node;
        node.prev.next = node;
    }

    private void removeNode(@NotNull Node<K, V> node) {
        node.next.prev = node.prev;
        node.prev.next = node.next;
    }

    static final class Node<K, V> {
        K key;
        V value;
        Node<K, V> prev;
        Node<K, V> next;

        public Node(K key, V value) {
            this.key = key;
            this.value = value;
        }

        static <K, V> @NotNull Node<K, V> createDummyNode() {
            // set key and value to null because dummy node key and value never be accessed
            Node<K, V> dummy = new Node<>(null, null);
            dummy.next = dummy;
            dummy.prev = dummy;
            return dummy;
        }
    }
}
