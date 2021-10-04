package com.muldersoft.maps.sqlite.test;

import java.util.Objects;
import java.util.Map.Entry;

public class MutableEntry<K,V> implements Entry<K,V> {
    private K key;
    private V value;

    MutableEntry() { }

    public MutableEntry(final K key, final V value) {
        this.key = Objects.requireNonNull(key);
        this.value = Objects.requireNonNull(value);
    }

    @Override
    public K getKey() {
        return key;
    }

    @Override
    public V getValue() {
        return value;
    }

    public K setKey(final K key) {
        final K oldKey = this.key;
        this.key = Objects.requireNonNull(key);
        return oldKey;
    }

    @Override
    public V setValue(final V value) {
        final V oldValue = this.value;
        this.value = Objects.requireNonNull(value);
        return oldValue;
    }
}
