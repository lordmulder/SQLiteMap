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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import javax.crypto.Cipher;

public abstract class AbstractTest {

    private static final char[] HEX_CHARS = "0123456789ABCDEF".toCharArray();

    // ======================================================================
    // Protected Methods
    // ======================================================================

    protected static <T> void assertCollectionEq(final Collection<T> collection0, final Collection<T> collection1) {
        final Map<T, Integer> counts0 = countKeys(collection0);
        final Map<T, Integer> counts1 = countKeys(collection1);
        assertCountsMatch(counts0, counts1);
        assertCountsMatch(counts1, counts0);
    }

    protected static int[] shuffle(final int[] array, final SecureRandom secureRandom) {
        for (int i = array.length; i > 1; --i) {
            swap(array, i - 1, secureRandom.nextInt(i));
        }
        return array;
    }

    protected static byte[] cryptToBytes(final Cipher cipher, final long value) throws SecurityException {
        try {
            return cipher.doFinal(ByteBuffer.allocate(Long.BYTES).putLong(value).array());
        } catch (final Exception e) {
            throw new SecurityException("Failed to encrypt or decrypt value!");
        }
    }

    protected static long cryptToLong(final Cipher cipher, final byte[] value) throws SecurityException {
        Objects.requireNonNull(value, "Value must not be null!");
        try {
            return ByteBuffer.wrap(cipher.doFinal(value)).getLong();
        } catch (final Exception e) {
            throw new SecurityException("Failed to encrypt or decrypt value!");
        }
    }

    protected static long cryptToLong(final Cipher cipher, final long value) throws SecurityException {
        try {
            return ByteBuffer.wrap(cipher.doFinal(ByteBuffer.allocate(Long.BYTES).putLong(value).array())).getLong();
        } catch (final Exception e) {
            throw new SecurityException("Failed to encrypt or decrypt value!");
        }
    }

    protected static String bytesToHexStr(final byte[] bytes) {
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

    // ======================================================================
    // Private Methods
    // ======================================================================

    private static <T> void assertCountsMatch(final Map<T, Integer> counts0, final Map<T, Integer> counts1) {
        final Integer defaultValue = Integer.valueOf(Integer.MIN_VALUE);
        for (final Entry<T, Integer> element : counts0.entrySet()) {
            assertEquals(element.getValue(), counts1.getOrDefault(element.getKey(), defaultValue));
        }
    }

    private static <T> Map<T, Integer> countKeys(final Collection<T> collection) {
        final Map<T, Counter> counts = new HashMap<T, Counter>(collection.size());
        for (final T element : collection) {
            final Counter counter = counts.computeIfAbsent(element, k -> new Counter());
            counter.increment();
        }
        return Collections.unmodifiableMap(counts.entrySet().stream().collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue().intValue())));
    }

    private static void swap(final int[] array, final int i, final int j) {
        int t = array[i];
        array[i] = array[j];
        array[j] = t;
    }

    // --------------------------------------------------------
    // Helper Class
    // --------------------------------------------------------

    private static class Counter {
        private int value = 0;

        public void increment() {
            value = Math.addExact(value, 1);
        }

        public int intValue() {
            return value;
        }
    }
}
