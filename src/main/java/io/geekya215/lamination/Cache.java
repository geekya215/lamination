package io.geekya215.lamination;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public interface Cache<K, V> {
    void put(@NotNull K key, @NotNull V value);

    @Nullable V get(@NotNull K key);
}
