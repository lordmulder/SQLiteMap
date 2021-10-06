/*
 * SQLiteMap was created by LoRd_MuldeR <mulder2@gmx.de>.
 *
 * To the extent possible under law, the person who associated CC0 with SQLiteMap has waived all copyright and related or
 * neighboring rights to SQLiteMap. You should have received a copy of the CC0 legalcode along with this work.
 *
 * If not, please refer to:
 * http://creativecommons.org/publicdomain/zero/1.0/
 */
package com.muldersoft.container.sqlite.test;

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
