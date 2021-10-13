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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigInteger;
import java.security.SecureRandom;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.TestMethodOrder;

import com.muldersoft.container.sqlite.SQLiteSet;
import com.muldersoft.container.sqlite.SQLiteSet.IterationOrder;

@TestMethodOrder(OrderAnnotation.class)
public class SQLiteSetTest  extends AbstractTest {

    private static final Set<String> REFERENCE;
    static {
        final Set<String> builder = new LinkedHashSet<String>();
        builder.add("foo");
        builder.add("bar");
        builder.add("baz");
        builder.add("qux");
        REFERENCE = Collections.unmodifiableSet(builder);
    }

    private static final List<String> ALPHABET = Collections.unmodifiableList(Arrays.asList(
        "Anton", "Berta", "Caesar", "Dora", "Emil", "Friedrich", "Gustav", "Heinrich", "Ida", "Julius", "Kaufmann", "Ludwig", "Martha", "Nordpol",
        "Otto", "Paula", "Quelle", "Richard", "Samuel", "Theodor", "Ulrich", "Viktor", "Wilhelm", "Xanthippe", "Ypsilon", "Zacharias"));

    // ======================================================================
    // Test Cases
    // ======================================================================

    @RepeatedTest(5)
    @Order(0)
    public void testTypes() {
        final Class<?>[] types = new Class<?>[] { Boolean.class, String.class, Byte.class, byte[].class, Integer.class, Long.class, Instant.class, BigInteger.class };
        for (int i = 0; i < types.length; ++i) {
            try(final SQLiteSet<?> set = SQLiteSet.fromMemory(types[i])) {
                assertEquals(types[i], set.getType());
                assertNull(set.getPath());
                assertNotNull(set.getTableName());
                System.out.println(set.getTableName());
            }
        }
        for (int i = 0; i < types.length; ++i) {
            try(final SQLiteSet<?> set = SQLiteSet.fromFile(types[i])) {
                assertEquals(types[i], set.getType());
                assertNotNull(set.getPath());
                assertNotNull(set.getTableName());
                System.out.printf("%s <- %s%n", set.getTableName(), set.getPath());
            }
        }
    }

    @RepeatedTest(5)
    @Order(1)
    public void testAdd() {
        try(final SQLiteSet<String> set = SQLiteSet.fromMemory(String.class)) {
            for (final String ref : REFERENCE) {
                assertTrue(set.add(ref));
            }
            assertCollectionEq(REFERENCE, set);
        }
    }

    @RepeatedTest(5)
    @Order(2)
    public void testAddReplace() {
        final Set<String> reference = new HashSet<String>(REFERENCE);
        final boolean expected0 = reference.add("bar");
        final boolean expected1 = reference.add("xyz");
        try(final SQLiteSet<String> set = SQLiteSet.fromMemory(String.class)) {
            for (final String ref : REFERENCE) {
                assertTrue(set.add(ref));
            }
            final boolean previous0 = set.add("bar");
            final boolean previous1 = set.add("xyz");
            assertEquals(expected0, previous0);
            assertEquals(expected1, previous1);
            assertCollectionEq(reference, set);
        }
    }

    @RepeatedTest(5)
    @Order(3)
    public void testAdd0() {
        try(final SQLiteSet<String> set = SQLiteSet.fromMemory(String.class)) {
            for (final String ref : REFERENCE) {
                set.add0(ref);
            }
            assertCollectionEq(REFERENCE, set);
        }
    }

    @RepeatedTest(5)
    @Order(4)
    public void testAdd0Replace() {
        final Set<String> reference = new HashSet<String>(REFERENCE);
        reference.add("bar");
        reference.add("xyz");
        try(final SQLiteSet<String> set = SQLiteSet.fromMemory(String.class)) {
            for (final String ref : REFERENCE) {
                set.add0(ref);
            }
            set.add0("bar");
            set.add0("xyz");
            assertCollectionEq(reference, set);
        }
    }

    @RepeatedTest(5)
    @Order(5)
    public void testAddAll() {
        final Set<String> reference = new HashSet<String>(REFERENCE);
        final Collection<String> addElements = Arrays.asList("bar", "xyz");
        final boolean expected0 = reference.addAll(addElements);
        final boolean expected1 = reference.addAll(addElements);
        try(final SQLiteSet<String> set = SQLiteSet.fromMemory(String.class)) {
            for (final String ref : REFERENCE) {
                set.add0(ref);
            }
            assertEquals(expected0, set.addAll(addElements));
            assertEquals(expected1, set.addAll(addElements));
            assertCollectionEq(reference, set);
        }
    }

    @RepeatedTest(5)
    @Order(6)
    public void testAddAll0() {
        final Set<String> reference = new HashSet<String>(REFERENCE);
        final Collection<String> addElements = Arrays.asList("bar", "xyz");
        reference.addAll(addElements);
        try(final SQLiteSet<String> set = SQLiteSet.fromMemory(String.class)) {
            for (final String ref : REFERENCE) {
                set.add0(ref);
            }
            set.addAll0(addElements);
            assertCollectionEq(reference, set);
        }
    }

    @RepeatedTest(5)
    @Order(7)
    public void testContains() {
        try(final SQLiteSet<String> set = SQLiteSet.fromMemory(String.class)) {
            for (final String ref : REFERENCE) {
                set.add0(ref);
            }
            assertFalse(set.contains(null));
            assertFalse(set.contains((Object)Integer.valueOf(42)));
            assertTrue (set.contains("bar"));
            assertTrue (set.contains("baz"));
            assertFalse(set.contains("xyz"));
            assertFalse(set.contains("lol"));
            set.add0("xyz");
            assertTrue (set.contains("bar"));
            assertTrue (set.contains("baz"));
            assertTrue (set.contains("xyz"));
            assertFalse(set.contains("lol"));
            set.add0("lol");
            assertTrue (set.contains("bar"));
            assertTrue (set.contains("baz"));
            assertTrue (set.contains("xyz"));
            assertTrue (set.contains("lol"));
            set.remove0("baz");
            assertTrue (set.contains("bar"));
            assertFalse(set.contains("baz"));
            assertTrue (set.contains("xyz"));
            assertTrue (set.contains("lol"));
            set.remove0("bar");
            assertFalse(set.contains("bar"));
            assertFalse(set.contains("baz"));
            assertTrue (set.contains("xyz"));
            assertTrue (set.contains("lol"));
        }
    }

    @RepeatedTest(5)
    @Order(8)
    public void testContainsAll() {
        final Set<String> reference = new HashSet<String>(REFERENCE);
        try(final SQLiteSet<String> set = SQLiteSet.fromMemory(String.class)) {
            for (final String ref : REFERENCE) {
                set.add0(ref);
            }
            assertTrue (set.containsAll(reference));
            reference.add("xyz");
            assertFalse(set.containsAll(reference));
            set.add0("xyz");
            assertTrue (set.containsAll(reference));
        }
    }

    @RepeatedTest(5)
    @Order(9)
    public void testRemove() {
        final Set<String> reference = new HashSet<String>(REFERENCE);
        final boolean expected0 = reference.remove("bar");
        final boolean expected1 = reference.remove("baz");
        final boolean expected2 = reference.remove("xyz");
        try(final SQLiteSet<String> set = SQLiteSet.fromMemory(String.class)) {
            for (final String ref : REFERENCE) {
                set.add0(ref);
            }
            assertEquals(expected0, set.remove("bar"));
            assertEquals(expected1, set.remove("baz"));
            assertEquals(expected2, set.remove("xyz"));
            assertCollectionEq(reference, set);
        }
    }

    @RepeatedTest(5)
    @Order(10)
    public void testRemove0() {
        final Set<String> reference = new HashSet<String>(REFERENCE);
        reference.remove("bar");
        reference.remove("baz");
        reference.remove("xyz");
        try(final SQLiteSet<String> set = SQLiteSet.fromMemory(String.class)) {
            for (final String ref : REFERENCE) {
                set.add0(ref);
            }
            set.remove0("bar");
            set.remove0("baz");
            set.remove0("xyz");
            assertCollectionEq(reference, set);
        }
    }

    @RepeatedTest(5)
    @Order(11)
    public void testRemoveAll() {
        final Collection<String> collection0 = Arrays.asList("bar", "baz", "xyz");
        final Collection<String> collection1 = Arrays.asList("xyz", "lol", "omg");
        final Set<String> reference = new HashSet<String>(REFERENCE);
        final boolean expected0 = reference.removeAll(collection0);
        final boolean expected1 = reference.removeAll(collection1);
        try(final SQLiteSet<String> set = SQLiteSet.fromMemory(String.class)) {
            for (final String ref : REFERENCE) {
                set.add0(ref);
            }
            assertEquals(expected0, set.removeAll(collection0));
            assertEquals(expected1, set.removeAll(collection1));
            assertCollectionEq(reference, set);
        }
    }

    @RepeatedTest(5)
    @Order(12)
    public void testRemoveAll0() {
        final Collection<String> collection0 = Arrays.asList("bar", "baz", "xyz");
        final Collection<String> collection1 = Arrays.asList("xyz", "lol", "omg");
        final Set<String> reference = new HashSet<String>(REFERENCE);
        reference.removeAll(collection0);
        reference.removeAll(collection1);
        try(final SQLiteSet<String> set = SQLiteSet.fromMemory(String.class)) {
            for (final String ref : REFERENCE) {
                set.add0(ref);
            }
            set.removeAll0(collection0);
            set.removeAll0(collection1);
            assertCollectionEq(reference, set);
        }
    }

    @RepeatedTest(5)
    @Order(13)
    public void testClear() {
        final Set<String> reference = new HashSet<String>(REFERENCE);
        reference.clear();
        reference.add("xyz");
        reference.add("lol");
        try(final SQLiteSet<String> set = SQLiteSet.fromMemory(String.class)) {
            for (final String ref : REFERENCE) {
                set.add0(ref);
            }
            assertCollectionEq(REFERENCE, set);
            set.clear();
            assertCollectionEq(Collections.emptySet(), set);
            set.add0("xyz");
            set.add0("lol");
            assertCollectionEq(reference, set);
        }
    }

    @RepeatedTest(5)
    @Order(14)
    public void testSize() {
        final int expected = REFERENCE.size();
        try(final SQLiteSet<String> set = SQLiteSet.fromMemory(String.class)) {
            for (final String ref : REFERENCE) {
                set.add0(ref);
            }
            assertEquals(expected, set.size());
            assertEquals(expected, set.sizeLong());
            set.add0("bar");
            assertEquals(expected, set.size());
            assertEquals(expected, set.sizeLong());
            set.add0("xyz");
            assertEquals(expected + 1, set.size());
            assertEquals(expected + 1, set.sizeLong());
            set.clear();
            assertEquals(0, set.size());
            assertEquals(0, set.sizeLong());
        }
    }

    @RepeatedTest(5)
    @Order(15)
    public void testIsEmpty() {
        try(final SQLiteSet<String> set = SQLiteSet.fromMemory(String.class)) {
            assertTrue(set.isEmpty());
            for (final String ref : REFERENCE) {
                set.add0(ref);
            }
            assertFalse(set.isEmpty());
            set.clear();
            assertTrue (set.isEmpty());
        }
    }

    @RepeatedTest(5)
    @Order(16)
    public void testIterator() {
        try(final SQLiteSet<String> set = SQLiteSet.fromMemory(String.class)) {
            for (final String ref : REFERENCE) {
                set.add0(ref);
            }
            final Set<String> entries = new LinkedHashSet<String>();
            try (final SQLiteSet<String>.SQLiteSetIterator iter = set.iterator()) {
                while (iter.hasNext()) {
                    entries.add(iter.next());
                }
            }
            assertCollectionEq(REFERENCE, entries);
        }
    }

    @RepeatedTest(5)
    @Order(17)
    public void testIteratorOrderBy() {
        final List<String> keysDescending = new ArrayList<String>(ALPHABET);
        final List<String> keysRandomized = new ArrayList<String>(ALPHABET);
        Collections.reverse(keysDescending);
        Collections.shuffle(keysRandomized, ThreadLocalRandom.current());
        try(final SQLiteSet<String> set = SQLiteSet.fromMemory(String.class)) {
            for (final String key : keysRandomized) {
                set.add0(key);
            }
            try (final SQLiteSet<String>.SQLiteSetIterator iter = set.iterator(IterationOrder.ASCENDING)) {
                final Iterator<String> expected = ALPHABET.iterator();
                while (expected.hasNext()) {
                    assertTrue(iter.hasNext());
                    assertEquals(expected.next(), iter.next());
                }
            }
            try (final SQLiteSet<String>.SQLiteSetIterator iter = set.iterator(IterationOrder.DESCENDING)) {
                final Iterator<String> expected = keysDescending.iterator();
                while (expected.hasNext()) {
                    assertTrue(iter.hasNext());
                    assertEquals(expected.next(), iter.next());
                }
            }
        }
    }

    @RepeatedTest(5)
    @Order(18)
    public void testIteratorDefaultKeyOrder() {
        final List<String> keysDescending = new ArrayList<String>(ALPHABET);
        final List<String> keysRandomized = new ArrayList<String>(ALPHABET);
        Collections.reverse(keysDescending);
        Collections.shuffle(keysRandomized, ThreadLocalRandom.current());
        try(final SQLiteSet<String> set = SQLiteSet.fromMemory(String.class)) {
            for (final String key : keysRandomized) {
                set.add0(key);
            }
            assertEquals(IterationOrder.UNSPECIFIED, set.setDefaultOrder(IterationOrder.ASCENDING));
            try (final SQLiteSet<String>.SQLiteSetIterator iter = set.iterator()) {
                final Iterator<String> expected = ALPHABET.iterator();
                while (expected.hasNext()) {
                    assertTrue(iter.hasNext());
                    assertEquals(expected.next(), iter.next());
                }
            }
            assertEquals(IterationOrder.ASCENDING, set.setDefaultOrder(IterationOrder.DESCENDING));
            try (final SQLiteSet<String>.SQLiteSetIterator iter = set.iterator()) {
                final Iterator<String> expected = keysDescending.iterator();
                while (expected.hasNext()) {
                    assertTrue(iter.hasNext());
                    assertEquals(expected.next(), iter.next());
                }
            }
        }
    }

    @RepeatedTest(5)
    @Order(19)
    public void testHashCode() {
        try(final SQLiteSet<String> set = SQLiteSet.fromMemory(String.class)) {
            for (final String ref : REFERENCE) {
                set.add0(ref);
            }
            final int hash0a = set.hashCode();
            final int hash0b = set.hashCode();
            set.add("xyz");
            final int hash1a = set.hashCode();
            final int hash1b = set.hashCode();
            set.clear();
            final int hash2a = set.hashCode();
            final int hash2b = set.hashCode();
            assertEquals(hash0a, hash0b);
            assertEquals(hash1a, hash1b);
            assertEquals(hash2a, hash2b);
            assertNotEquals(hash0a, hash1a);
            assertNotEquals(hash1a, hash2a);
            assertNotEquals(hash2a, hash0a);
        }
    }

    @RepeatedTest(5)
    @Order(20)
    public void testEquals() {
        final Set<String> reference = new HashSet<String>(REFERENCE);
        try(final SQLiteSet<String> set = SQLiteSet.fromMemory(String.class)) {
            for (final String ref : REFERENCE) {
                set.add0(ref);
            }
            assertTrue (set.equals(set));
            assertFalse(set.equals((Object)Integer.valueOf(42)));
            assertTrue (set.equals(reference));
            set.add0("xzy");
            assertFalse(set.equals(reference));
            reference.add("lol");
            assertFalse(set.equals(reference));
            reference.remove("lol");
            reference.add("xzy");
            assertTrue (set.equals(reference));
            set.remove("bar");
            assertFalse(set.equals(reference));
            reference.remove("bar");
            assertTrue (set.equals(reference));
        }
    }

    @RepeatedTest(5)
    @Order(21)
    public void testForEach() {
        final Set<String> entries = new LinkedHashSet<String>();
        try(final SQLiteSet<String> set = SQLiteSet.fromMemory(String.class)) {
            for (final String ref : REFERENCE) {
                set.add0(ref);
            }
            set.forEach(e -> entries.add(e));
        }
        assertCollectionEq(REFERENCE, entries);
    }

    @RepeatedTest(5)
    @Order(22)
    public void testSpliterator() throws Exception {
        try(final SQLiteSet<String> set = SQLiteSet.fromMemory(String.class)) {
            for (final String ref : REFERENCE) {
                set.add0(ref);
            }
            assertThrows(UnsupportedOperationException.class, () -> set.spliterator());
        }
    }

    @RepeatedTest(5)
    @Order(23)
    public void testStream() throws Exception {
        try(final SQLiteSet<String> set = SQLiteSet.fromMemory(String.class)) {
            for (final String ref : REFERENCE) {
                set.add0(ref);
            }
            assertThrows(UnsupportedOperationException.class, () -> set.stream());
        }
    }

    @RepeatedTest(5)
    @Order(24)
    public void testClose() throws Exception {
        final SQLiteSet<String> set = SQLiteSet.fromMemory(String.class);
        set.close();
        assertThrows(IllegalStateException.class, () -> set.add("xyz"));
    }

    @RepeatedTest(5)
    @Order(25)
    public void testRandomStrings() throws Exception {
        final SecureRandom secureRandom = new SecureRandom();
        final SecretKey desKey = new SecretKeySpec(secureRandom.generateSeed(Long.BYTES), "DES");
        final Cipher cipher = Cipher.getInstance("DES/ECB/NoPadding");
        try(final SQLiteSet<String> set = SQLiteSet.fromMemory(String.class)) {
            final int COUNT = 0x10000;
            cipher.init(Cipher.ENCRYPT_MODE, desKey);
            for (int i = 0; i < COUNT; ++i) {
                final long element = cryptToLong(cipher, i);
                System.out.printf("%04X -> %016X%n", i, element);
                set.add0(String.format("%016X", element));
            }
            final int[] indices = shuffle(IntStream.range(0, COUNT).toArray(), secureRandom);
            for (final int i : indices) {
                final long element0 = cryptToLong(cipher, i);
                assertTrue (set.contains(String.format("%016X", element0)));
                final long element1 = cryptToLong(cipher, COUNT + i);
                assertFalse(set.contains(String.format("%016X", element1)));
                final long element2 = cryptToLong(cipher, i - COUNT);
                assertFalse(set.contains(String.format("%016X", element2)));
                System.out.printf("%016X -> %04X%n", element0, i);
            }
        }
    }

    @RepeatedTest(5)
    @Order(26)
    public void testRandomBytes() throws Exception {
        final SecureRandom secureRandom = new SecureRandom();
        final SecretKey desKey = new SecretKeySpec(secureRandom.generateSeed(Long.BYTES), "DES");
        final Cipher cipher = Cipher.getInstance("DES/ECB/NoPadding");
        try(final SQLiteSet<byte[]> set = SQLiteSet.fromMemory(byte[].class)) {
            final int COUNT = 0x10000;
            cipher.init(Cipher.ENCRYPT_MODE, desKey);
            for (int i = 0; i < COUNT; ++i) {
                final byte[] element = cryptToBytes(cipher, i);
                System.out.printf("%04X -> %s%n", i, bytesToHexStr(element));
                set.add0(element);
            }
            final int[] indices = shuffle(IntStream.range(0, COUNT).toArray(), secureRandom);
            for (final int i : indices) {
                final byte[] element0 = cryptToBytes(cipher, i);
                assertTrue (set.contains(element0));
                final byte[] element1 = cryptToBytes(cipher, COUNT + i);
                assertFalse(set.contains(element1));
                final byte[] element2 = cryptToBytes(cipher, i - COUNT);
                assertFalse(set.contains(element2));
                System.out.printf("%s -> %04X%n", bytesToHexStr(element0), i);
            }
        }
    }
}
