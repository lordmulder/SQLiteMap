/*
 * SQLiteMap was created by LoRd_MuldeR <mulder2@gmx.de>.
 *
 * To the extent possible under law, the person who associated CC0 with SQLiteMap has waived all copyright and related or
 * neighboring rights to SQLiteMap. You should have received a copy of the CC0 legalcode along with this work.
 *
 * If not, please refer to:
 * http://creativecommons.org/publicdomain/zero/1.0/
 */
package com.muldersoft.container.sqlite.test.extra;

import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.TreeSet;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;

import com.muldersoft.container.sqlite.SQLiteMap;

public class StressTest {

    private static final int LIMIT = 100000000;
    private static final int UPDATE_INTERVAL = 1000;
    private static final int BATCH_SIZE = 256;

    private static final char[] HEX_CHARS = "0123456789ABCDEF".toCharArray();

    // ======================================================================
    // Main
    // ======================================================================

    public static void main(String[] args) throws Exception {
        printMemoryStats();
        System.out.printf("Average time: %,d%n", measure(5, StressTest::runTest));
        System.gc();
        printMemoryStats();
    }
    
    private static void runTest() {
        try {
            final Cipher cipher0 = Cipher.getInstance("AES/ECB/NoPadding");
            final Cipher cipher1 = Cipher.getInstance("AES/ECB/NoPadding");
            final SecureRandom secureRandom = new SecureRandom();
            cipher0.init(Cipher.ENCRYPT_MODE, new SecretKeySpec(secureRandom.generateSeed(32), "AES"));
            cipher1.init(Cipher.ENCRYPT_MODE, new SecretKeySpec(secureRandom.generateSeed(32), "AES"));
            final MutableEntry<String, String>[] data = allocateBuffer(BATCH_SIZE);
            try (final SQLiteMap<String, String> map = SQLiteMap.fromMemory(String.class, String.class)) {
                final List<MutableEntry<String, String>> list = Arrays.asList(data);
                final ByteBuffer buffer = ByteBuffer.allocate(16);
                long currentTime = -1L, nextUpdate = System.currentTimeMillis() + UPDATE_INTERVAL;
                for (int counter = 0; counter < LIMIT; counter += data.length) {
                    buffer.putInt(12, counter);
                    for (int i = 0; i < data.length; ++i) {
                        buffer.put(15, (byte)i);
                        data[i].setKey  (bytesToHex(cipher0.doFinal(buffer.array())));
                        data[i].setValue(bytesToHex(cipher1.doFinal(buffer.array())));
                    }
                    map.putAll0(list);
                    if ((currentTime = System.currentTimeMillis()) >= nextUpdate) {
                        System.out.printf("%,d%n", map.size());
                        nextUpdate = currentTime + UPDATE_INTERVAL;
                    }
                }
                System.out.printf("%,d%n", map.size());
            }
        } catch (final Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    // ======================================================================
    // Private Methods
    // ======================================================================

    private static void printMemoryStats() {
        final Runtime runtime = Runtime.getRuntime();
        System.out.printf("Memory: free=%,d, total=%,d, max=%,d%n", runtime.freeMemory(), runtime.totalMemory(), runtime.maxMemory());
    }

    private static long measure(final int count, final Runnable runnable) {
        if ((count < 5) || (count % 2 != 1)) {
            throw new IllegalArgumentException("Invalid count specified!");
        }
        final TreeSet<Long> runs = new TreeSet<Long>();
        for (int r = 0; r < count; ++r) {
            System.out.printf("Run %d of %d, please wait...%n", r + 1, count);
            final long clock0 = System.nanoTime();
            runnable.run();
            final long clock1 = System.nanoTime();
            runs.add(clock1 - clock0);
        }
        final int skip = runs.size() / 4, limit = runs.size() - (2 * skip);
        return Math.round(runs.stream().mapToLong(Long::longValue).skip(skip).limit(limit).average().orElse(-1));
    }

    private static String bytesToHex(final byte[] bytes) {
        Objects.requireNonNull(bytes);
        final char[] buffer = new char[bytes.length * 2];
        int pos = 0;
        for (int i = 0; i < bytes.length; ++i) {
            int value = bytes[i] & 0xFF;
            buffer[pos++] = HEX_CHARS[value >>> 4];
            buffer[pos++] = HEX_CHARS[value & 0xF];
        }
        return new String(buffer);
    }

    @SuppressWarnings("unchecked")
    private static <K, V> MutableEntry<K, V>[] allocateBuffer(final int length) {
        final MutableEntry<K, V>[] array = (MutableEntry<K, V>[]) allocateArray(MutableEntry.class, length);
        for (int i = 0; i < length; ++i) {
            array[i] = new MutableEntry<K, V>();
        }
        return array;
    }

    @SuppressWarnings("unchecked")
    private static <T> T[] allocateArray(final Class<T> clazz, final int length) {
        return (T[]) Array.newInstance(clazz, length);
    }

    // ======================================================================
    // MutableEntry Class
    // ======================================================================

    private static class MutableEntry<K,V> implements Entry<K,V> {
        private K key;
        private V value;

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
}
