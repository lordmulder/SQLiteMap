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
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.TestMethodOrder;

import com.muldersoft.container.sqlite.SQLiteMap;

@TestMethodOrder(OrderAnnotation.class)
public class SQLiteMapTest extends AbstractTest {

    private static final Map<String, String> REFERENCE;
    static {
        final Map<String, String> builder = new HashMap<String, String>();
        builder.put("foo", "lorem ipsum");
        builder.put("bar", "ipsumsit amet");
        builder.put("baz", "consetetur sadipscing elitr");
        builder.put("qux", "sed diam nonumy");
        REFERENCE = Collections.unmodifiableMap(builder);
    }

    private static final String EXTRA_VALUE1 = "ut labore et dolore";
    private static final String EXTRA_VALUE0 = "eirmod tempor invidunt";

    // ======================================================================
    // Test Cases
    // ======================================================================

    @RepeatedTest(5)
    @Order(0)
    public void testTypes() {
        final Class<?>[] types = new Class<?>[] { Boolean.class, String.class, Byte.class, byte[].class, Integer.class, Long.class, Instant.class, BigInteger.class };
        for (int i = 0; i < types.length; ++i) {
            for (int j = 0; j < types.length; ++j) {
                try(final SQLiteMap<?, ?> map = SQLiteMap.fromMemory(types[i], types[j])) {
                    assertEquals(types[i], map.getKeyType());
                    assertEquals(types[j], map.getValueType());
                    assertNull(map.getPath());
                    assertNotNull(map.getTableName());
                    System.out.println(map.getTableName());
                }
            }
        }
        for (int i = 0; i < types.length; ++i) {
            try(final SQLiteMap<?, ?> map = SQLiteMap.fromFile(types[i], types[i])) {
                assertEquals(map.getKeyType(),   types[i]);
                assertEquals(map.getValueType(), types[i]);
                assertNotNull(map.getPath());
                assertNotNull(map.getTableName());
                System.out.printf("%s <- %s%n", map.getTableName(),  map.getPath());
            }
        }
    }

    @RepeatedTest(5)
    @Order(1)
    public void testPut() {
        try(final SQLiteMap<String, String> map = SQLiteMap.fromMemory(String.class, String.class)) {
            for (final Entry<String, String> ref : REFERENCE.entrySet()) {
                assertNull(map.put(ref.getKey(), ref.getValue()));
            }
            assertMapEq(REFERENCE, map);
        }
    }

    @RepeatedTest(5)
    @Order(2)
    public void testPutReplace() {
        final Map<String, String> reference = new HashMap<String, String>(REFERENCE);
        final String expected0 = reference.put("bar", EXTRA_VALUE0);
        final String expected1 = reference.put("xyz", EXTRA_VALUE1);
        try(final SQLiteMap<String, String> map = SQLiteMap.fromMemory(String.class, String.class)) {
            for (final Entry<String, String> ref : REFERENCE.entrySet()) {
                assertNull(map.put(ref.getKey(), ref.getValue()));
            }
            final String previous0 = map.put("bar", EXTRA_VALUE0);
            final String previous1 = map.put("xyz", EXTRA_VALUE1);
            assertEquals(expected0, previous0);
            assertEquals(expected1, previous1);
            assertMapEq(reference, map);
        }
    }

    @RepeatedTest(5)
    @Order(3)
    public void testPut0() {
        try(final SQLiteMap<String, String> map = SQLiteMap.fromMemory(String.class, String.class)) {
            for (final Entry<String, String> ref : REFERENCE.entrySet()) {
                map.put0(ref.getKey(), ref.getValue());
            }
            assertMapEq(REFERENCE, map);
        }
    }

    @RepeatedTest(5)
    @Order(4)
    public void testPut0Replace() {
        final Map<String, String> reference = new HashMap<String, String>(REFERENCE);
        reference.put("bar", EXTRA_VALUE0);
        reference.put("baz", EXTRA_VALUE1);
        try(final SQLiteMap<String, String> map = SQLiteMap.fromMemory(String.class, String.class)) {
            for (final Entry<String, String> ref : REFERENCE.entrySet()) {
                map.put0(ref.getKey(), ref.getValue());
            }
            map.put0("bar", EXTRA_VALUE0);
            map.put0("baz", EXTRA_VALUE1);
            assertMapEq(reference, map);
        }
    }

    @RepeatedTest(5)
    @Order(5)
    public void testPutAll() {
        try(final SQLiteMap<String, String> map = SQLiteMap.fromMemory(String.class, String.class)) {
            map.putAll(REFERENCE);
            assertMapEq(REFERENCE, map);
        }
    }

    @RepeatedTest(5)
    @Order(6)
    public void testPutAllEntryCollection() {
        final Map<String, String> reference = new HashMap<String, String>(REFERENCE);
        reference.putIfAbsent("bar", EXTRA_VALUE0);
        reference.putIfAbsent("xyz", EXTRA_VALUE1);
        try(final SQLiteMap<String, String> map = SQLiteMap.fromMemory(String.class, String.class)) {
            assertTrue (map.putAll(REFERENCE.entrySet()));
            assertMapEq(REFERENCE, map);
            assertTrue (map.putAll(reference.entrySet()));
            assertMapEq(reference, map);
            assertFalse(map.putAll(reference.entrySet()));
        }
    }

    @RepeatedTest(5)
    @Order(7)
    public void testPutAllKeyCollection() {
        final Map<String, String> reference0 = new HashMap<String, String>(REFERENCE);
        final Map<String, String> reference1 = new HashMap<String, String>(REFERENCE);
        reference0.replaceAll((key, value) -> EXTRA_VALUE0);
        reference1.replaceAll((key, value) -> EXTRA_VALUE1);
        try(final SQLiteMap<String, String> map = SQLiteMap.fromMemory(String.class, String.class)) {
            assertTrue (map.putAll(REFERENCE.keySet(), EXTRA_VALUE0));
            assertMapEq(reference0, map);
            assertFalse(map.putAll(REFERENCE.keySet(), EXTRA_VALUE1));
            assertMapEq(reference1, map);
        }
    }

    @RepeatedTest(5)
    @Order(8)
    public void testPutIfAbsent() {
        final Map<String, String> reference = new HashMap<String, String>(REFERENCE);
        final String expected0 = reference.putIfAbsent("bar", EXTRA_VALUE0);
        final String expected1 = reference.putIfAbsent("xyz", EXTRA_VALUE1);
        try(final SQLiteMap<String, String> map = SQLiteMap.fromMemory(String.class, String.class)) {
            for (final Entry<String, String> ref : REFERENCE.entrySet()) {
                map.put(ref.getKey(), ref.getValue());
            }
            final String previous0 = map.putIfAbsent("bar", EXTRA_VALUE0);
            final String previous1 = map.putIfAbsent("xyz", EXTRA_VALUE1);
            assertEquals(expected0, previous0);
            assertEquals(expected1, previous1);
            assertMapEq(reference, map);
        }
    }

    @RepeatedTest(5)
    @Order(9)
    public void testPutIfAbsent0() {
        final Map<String, String> reference = new HashMap<String, String>(REFERENCE);
        reference.putIfAbsent("bar", EXTRA_VALUE0);
        reference.putIfAbsent("xyz", EXTRA_VALUE1);
        try(final SQLiteMap<String, String> map = SQLiteMap.fromMemory(String.class, String.class)) {
            for (final Entry<String, String> ref : REFERENCE.entrySet()) {
                map.put(ref.getKey(), ref.getValue());
            }
            map.putIfAbsent0("bar", EXTRA_VALUE0);
            map.putIfAbsent0("xyz", EXTRA_VALUE1);
            assertMapEq(reference, map);
        }
    }

    @RepeatedTest(5)
    @Order(10)
    public void testPutAll0EntryCollection() {
        try(final SQLiteMap<String, String> map = SQLiteMap.fromMemory(String.class, String.class)) {
            map.putAll0(REFERENCE.entrySet());
            assertMapEq(REFERENCE, map);
        }
    }

    @RepeatedTest(5)
    @Order(11)
    public void testPutAll0KeyCollection() {
        final Map<String, String> reference = new HashMap<String, String>(REFERENCE);
        reference.replaceAll((key, value) -> EXTRA_VALUE0);
        try(final SQLiteMap<String, String> map = SQLiteMap.fromMemory(String.class, String.class)) {
            map.putAll0(REFERENCE.keySet(), EXTRA_VALUE0);
            assertMapEq(reference, map);
        }
    }

    @RepeatedTest(5)
    @Order(12)
    public void testGet() {
        try(final SQLiteMap<String, String> map = SQLiteMap.fromMemory(String.class, String.class)) {
            for (final Entry<String, String> ref : REFERENCE.entrySet()) {
                map.put0(ref.getKey(), ref.getValue());
            }
            assertEquals(REFERENCE.get("foo"), map.get("foo"));
            assertEquals(REFERENCE.get("qux"), map.get("qux"));
            assertEquals(REFERENCE.get("bar"), map.get("bar"));
            assertEquals(REFERENCE.get("baz"), map.get("baz"));
            assertNull(map.get("xyz"));
            assertNull(map.get((Object)Integer.valueOf(42)));
        }
    }

    @RepeatedTest(5)
    @Order(13)
    public void testGetOrDefault() {
        try(final SQLiteMap<String, String> map = SQLiteMap.fromMemory(String.class, String.class)) {
            for (final Entry<String, String> ref : REFERENCE.entrySet()) {
                map.put0(ref.getKey(), ref.getValue());
            }
            assertEquals(REFERENCE.get("bar"), map.getOrDefault("bar", EXTRA_VALUE0));
            assertEquals(REFERENCE.get("baz"), map.getOrDefault("baz", EXTRA_VALUE1));
            assertEquals(EXTRA_VALUE0, map.getOrDefault("xyz", EXTRA_VALUE0));
            assertEquals(EXTRA_VALUE1, map.getOrDefault((Object)Integer.valueOf(42), EXTRA_VALUE1));
        }
    }

    @RepeatedTest(5)
    @Order(14)
    public void testRemove() {
        final Map<String, String> reference = new HashMap<String, String>(REFERENCE);
        final String expected0 = reference.remove("bar");
        final String expected1 = reference.remove("baz");
        final String expected2 = reference.remove("xyz");
        try(final SQLiteMap<String, String> map = SQLiteMap.fromMemory(String.class, String.class)) {
            for (final Entry<String, String> ref : REFERENCE.entrySet()) {
                map.put0(ref.getKey(), ref.getValue());
            }
            assertEquals(expected0, map.remove("bar"));
            assertEquals(expected1, map.remove("baz"));
            assertEquals(expected2, map.remove("xyz"));
            assertMapEq(reference, map);
        }
    }

    @RepeatedTest(5)
    @Order(15)
    public void testRemove0() {
        final Map<String, String> reference = new HashMap<String, String>(REFERENCE);
        reference.remove("bar");
        reference.remove("baz");
        try(final SQLiteMap<String, String> map = SQLiteMap.fromMemory(String.class, String.class)) {
            for (final Entry<String, String> ref : REFERENCE.entrySet()) {
                map.put0(ref.getKey(), ref.getValue());
            }
            map.remove0("bar");
            map.remove0("baz");
            assertMapEq(reference, map);
        }
    }

    @RepeatedTest(5)
    @Order(16)
    public void testRemoveValue() {
        final Map<String, String> reference = new HashMap<String, String>(REFERENCE);
        final String oldValue = reference.get("bar");
        final boolean expected0 = reference.remove("bar", oldValue);
        final boolean expected1 = reference.remove("baz", oldValue);
        final boolean expected2 = reference.remove("xyz", oldValue);
        try(final SQLiteMap<String, String> map = SQLiteMap.fromMemory(String.class, String.class)) {
            for (final Entry<String, String> ref : REFERENCE.entrySet()) {
                map.put0(ref.getKey(), ref.getValue());
            }
            final boolean removed0 = map.remove("bar", oldValue);
            final boolean removed1 = map.remove("baz", oldValue);
            final boolean removed2 = map.remove("xyz", oldValue);
            assertEquals(expected0, removed0);
            assertEquals(expected1, removed1);
            assertEquals(expected2, removed2);
            assertMapEq(reference, map);
        }
    }

    @RepeatedTest(5)
    @Order(17)
    public void testRemove0Value() {
        final Map<String, String> reference = new HashMap<String, String>(REFERENCE);
        final String oldValue = reference.get("bar");
        reference.remove("bar", oldValue);
        reference.remove("baz", oldValue);
        reference.remove("xyz", oldValue);
        try(final SQLiteMap<String, String> map = SQLiteMap.fromMemory(String.class, String.class)) {
            for (final Entry<String, String> ref : REFERENCE.entrySet()) {
                map.put0(ref.getKey(), ref.getValue());
            }
            map.remove0("bar", oldValue);
            map.remove0("baz", oldValue);
            map.remove0("xyz", oldValue);
            assertMapEq(reference, map);
        }
    }

    @RepeatedTest(5)
    @Order(18)
    public void testRemoveAll() {
        final Collection<String> collection0 = Arrays.asList("bar", "baz", "xyz");
        final Collection<String> collection1 = Arrays.asList("xyz", "lol", "omg");
        final Map<String, String> reference = new HashMap<String, String>(REFERENCE);
        final boolean expected0 = reference.keySet().removeAll(collection0);
        final boolean expected1 = reference.keySet().removeAll(collection1);
        try(final SQLiteMap<String, String> map = SQLiteMap.fromMemory(String.class, String.class)) {
            for (final Entry<String, String> ref : REFERENCE.entrySet()) {
                map.put0(ref.getKey(), ref.getValue());
            }
            assertEquals(expected0, map.removeAll(collection0));
            assertEquals(expected1, map.removeAll(collection1));
            assertMapEq(reference, map);
        }
    }

    @RepeatedTest(5)
    @Order(19)
    public void testRemoveAll0() {
        final Collection<String> collection0 = Arrays.asList("bar", "baz", "xyz");
        final Collection<String> collection1 = Arrays.asList("xyz", "lol", "omg");
        final Map<String, String> reference = new HashMap<String, String>(REFERENCE);
        reference.keySet().removeAll(collection0);
        reference.keySet().removeAll(collection1);
        try(final SQLiteMap<String, String> map = SQLiteMap.fromMemory(String.class, String.class)) {
            for (final Entry<String, String> ref : REFERENCE.entrySet()) {
                map.put0(ref.getKey(), ref.getValue());
            }
            map.removeAll0(collection0);
            map.removeAll0(collection1);
            assertMapEq(reference, map);
        }
    }

    @RepeatedTest(5)
    @Order(20)
    public void testReplace() {
        final Map<String, String> reference = new HashMap<String, String>(REFERENCE);
        final String expected0 = reference.replace("bar", EXTRA_VALUE0);
        final String expected1 = reference.replace("xyz", EXTRA_VALUE1);
        try(final SQLiteMap<String, String> map = SQLiteMap.fromMemory(String.class, String.class)) {
            for (final Entry<String, String> ref : REFERENCE.entrySet()) {
                map.put0(ref.getKey(), ref.getValue());
            }
            assertEquals(expected0, map.replace("bar", EXTRA_VALUE0));
            assertEquals(expected1, map.replace("xyz", EXTRA_VALUE1));
            assertMapEq(reference, map);
        }
    }

    @RepeatedTest(5)
    @Order(21)
    public void testReplaceValue() {
        final Map<String, String> reference = new HashMap<String, String>(REFERENCE);
        final String oldValue = reference.get("bar");
        final boolean expected0 = reference.replace("bar", oldValue, EXTRA_VALUE0);
        final boolean expected1 = reference.replace("baz", oldValue, EXTRA_VALUE1);
        try(final SQLiteMap<String, String> map = SQLiteMap.fromMemory(String.class, String.class)) {
            for (final Entry<String, String> ref : REFERENCE.entrySet()) {
                map.put0(ref.getKey(), ref.getValue());
            }
            assertEquals(expected0, map.replace("bar", oldValue, EXTRA_VALUE0));
            assertEquals(expected1, map.replace("xyz", oldValue, EXTRA_VALUE1));
            assertMapEq(reference, map);
        }
    }

    @RepeatedTest(5)
    @Order(22)
    public void testReplace0() {
        final Map<String, String> reference = new HashMap<String, String>(REFERENCE);
        reference.replace("bar", EXTRA_VALUE0);
        reference.replace("xyz", EXTRA_VALUE1);
        try(final SQLiteMap<String, String> map = SQLiteMap.fromMemory(String.class, String.class)) {
            for (final Entry<String, String> ref : REFERENCE.entrySet()) {
                map.put0(ref.getKey(), ref.getValue());
            }
            map.replace0("bar", EXTRA_VALUE0);
            map.replace0("xyz", EXTRA_VALUE1);
            assertMapEq(reference, map);
        }
    }

    @RepeatedTest(5)
    @Order(23)
    public void testContainsKey() {
        try(final SQLiteMap<String, String> map = SQLiteMap.fromMemory(String.class, String.class)) {
            for (final Entry<String, String> ref : REFERENCE.entrySet()) {
                map.put0(ref.getKey(), ref.getValue());
            }
            assertFalse(map.containsKey(null));
            assertFalse(map.containsKey((Object)Integer.valueOf(42)));
            assertTrue (map.containsKey("bar"));
            assertTrue (map.containsKey("baz"));
            assertFalse(map.containsKey("xyz"));
            assertFalse(map.containsKey("lol"));
            map.put0("xyz", EXTRA_VALUE0);
            assertTrue (map.containsKey("bar"));
            assertTrue (map.containsKey("baz"));
            assertTrue (map.containsKey("xyz"));
            assertFalse(map.containsKey("lol"));
            map.put0("lol", EXTRA_VALUE1);
            assertTrue (map.containsKey("bar"));
            assertTrue (map.containsKey("baz"));
            assertTrue (map.containsKey("xyz"));
            assertTrue (map.containsKey("lol"));
            map.remove0("baz");
            assertTrue (map.containsKey("bar"));
            assertFalse(map.containsKey("baz"));
            assertTrue (map.containsKey("xyz"));
            assertTrue (map.containsKey("lol"));
            map.remove0("bar");
            assertFalse(map.containsKey("bar"));
            assertFalse(map.containsKey("baz"));
            assertTrue (map.containsKey("xyz"));
            assertTrue (map.containsKey("lol"));
        }
    }

    @RepeatedTest(5)
    @Order(24)
    public void testContainsAllKeys() {
        final Set<String> reference = new HashSet<String>(REFERENCE.keySet());
        try(final SQLiteMap<String, String> map = SQLiteMap.fromMemory(String.class, String.class)) {
            for (final Entry<String, String> ref : REFERENCE.entrySet()) {
                map.put0(ref.getKey(), ref.getValue());
            }
            assertTrue (map.containsAllKeys(reference));
            reference.add("xyz");
            assertFalse(map.containsAllKeys(reference));
            map.put0("xyz", EXTRA_VALUE0);
            assertTrue (map.containsAllKeys(reference));
        }
    }

    @RepeatedTest(5)
    @Order(25)
    public void testContainsValue() {
        try(final SQLiteMap<String, String> map = SQLiteMap.fromMemory(String.class, String.class)) {
            for (final Entry<String, String> ref : REFERENCE.entrySet()) {
                map.put0(ref.getKey(), ref.getValue());
            }
            assertFalse(map.containsValue(null));
            assertFalse(map.containsValue((Object)Integer.valueOf(42)));
            assertFalse(map.containsValue(EXTRA_VALUE0));
            assertFalse(map.containsValue(EXTRA_VALUE1));
            map.put0("bar", EXTRA_VALUE0);
            assertTrue (map.containsValue(EXTRA_VALUE0));
            assertFalse(map.containsValue(EXTRA_VALUE1));
            map.put0("bar", EXTRA_VALUE1);
            assertFalse(map.containsValue(EXTRA_VALUE0));
            assertTrue (map.containsValue(EXTRA_VALUE1));
            map.put0("baz", EXTRA_VALUE0);
            assertTrue (map.containsValue(EXTRA_VALUE0));
            assertTrue (map.containsValue(EXTRA_VALUE1));
        }
    }

    @RepeatedTest(5)
    @Order(26)
    public void testContainsAllValues() {
        final Collection<String> reference = new ArrayList<String>(REFERENCE.values());
        try(final SQLiteMap<String, String> map = SQLiteMap.fromMemory(String.class, String.class)) {
            for (final Entry<String, String> ref : REFERENCE.entrySet()) {
                map.put0(ref.getKey(), ref.getValue());
            }
            assertTrue (map.containsAllValues(reference));
            reference.add(EXTRA_VALUE0);
            assertFalse(map.containsAllValues(reference));
            map.put0("xyz", EXTRA_VALUE0);
            assertTrue (map.containsAllValues(reference));
        }
    }

    @RepeatedTest(5)
    @Order(27)
    public void testContainsEntry() {
        try(final SQLiteMap<String, String> map = SQLiteMap.fromMemory(String.class, String.class)) {
            for (final Entry<String, String> ref : REFERENCE.entrySet()) {
                map.put0(ref.getKey(), ref.getValue());
            }
            assertFalse(map.containsEntry(null));
            assertFalse(map.containsEntry(Integer.valueOf(42)));
            assertFalse(map.containsEntry(new SimpleEntry<String, Integer>("foo", 42)));
            for (final Entry<String, String> ref : REFERENCE.entrySet()) {
                assertTrue(map.containsEntry(ref));
            }
            assertTrue (map.containsEntry(new SimpleEntry<String, String>("foo", REFERENCE.get("foo"))));
            assertFalse(map.containsEntry(new SimpleEntry<String, String>("foo", EXTRA_VALUE0)));
            assertFalse(map.containsEntry(new SimpleEntry<String, String>("xyz", REFERENCE.get("foo"))));
        }
    }

    @RepeatedTest(5)
    @Order(28)
    public void testContainsAllEntries() {
        final Set<Entry<String, String>> reference = new HashSet<Entry<String, String>>(REFERENCE.entrySet());
        try(final SQLiteMap<String, String> map = SQLiteMap.fromMemory(String.class, String.class)) {
            for (final Entry<String, String> ref : REFERENCE.entrySet()) {
                map.put0(ref.getKey(), ref.getValue());
            }
            assertTrue (map.containsAllEntries(reference));
            reference.add(new SimpleEntry<String, String>("xzy", EXTRA_VALUE0));
            assertFalse(map.containsAllKeys(reference));
            map.put0("xzy", EXTRA_VALUE0);
            assertTrue (map.containsAllEntries(reference));
        }
    }

    @RepeatedTest(5)
    @Order(29)
    public void testCompute() {
        final AtomicReference<String> val0 = new AtomicReference<>(), val1 = new AtomicReference<>(), val2 = new AtomicReference<>();
        final Map<String, String> reference = new HashMap<String, String>(REFERENCE);
        reference.compute("bar", (k,v) -> { val0.set(v); return EXTRA_VALUE0; });
        reference.compute("xyz", (k,v) -> { val1.set(v); return EXTRA_VALUE1; });
        reference.compute("baz", (k,v) -> { val2.set(v); return null; });
        try(final SQLiteMap<String, String> map = SQLiteMap.fromMemory(String.class, String.class)) {
            for (final Entry<String, String> ref : REFERENCE.entrySet()) {
                map.put0(ref.getKey(), ref.getValue());
            }
            map.compute("bar", (k,v) -> { assertEquals(k, "bar"); assertEquals(v, val0.get()); return EXTRA_VALUE0; });
            map.compute("xyz", (k,v) -> { assertEquals(k, "xyz"); assertEquals(v, val1.get()); return EXTRA_VALUE1; });
            map.compute("baz", (k,v) -> { assertEquals(k, "baz"); assertEquals(v, val2.get()); return null; });
            assertMapEq(reference, map);
        }
    }

    @RepeatedTest(5)
    @Order(30)
    public void testComputeIfAbsent() {
        final AtomicInteger ref0 = new AtomicInteger(), ref1 = new AtomicInteger(), ref2 = new AtomicInteger();
        final AtomicInteger ctr0 = new AtomicInteger(), ctr1 = new AtomicInteger(), ctr2 = new AtomicInteger();
        final Map<String, String> reference = new HashMap<String, String>(REFERENCE);
        reference.computeIfAbsent("bar", k -> { ref0.incrementAndGet(); return EXTRA_VALUE0; });
        reference.computeIfAbsent("xyz", k -> { ref1.incrementAndGet(); return EXTRA_VALUE1; });
        reference.computeIfAbsent("lol", k -> { ref2.incrementAndGet(); return null; });
        try(final SQLiteMap<String, String> map = SQLiteMap.fromMemory(String.class, String.class)) {
            for (final Entry<String, String> ref : REFERENCE.entrySet()) {
                map.put0(ref.getKey(), ref.getValue());
            }
            map.computeIfAbsent("bar", k -> { ctr0.incrementAndGet(); assertEquals(k, "bar"); return EXTRA_VALUE0; });
            map.computeIfAbsent("xyz", k -> { ctr1.incrementAndGet(); assertEquals(k, "xyz"); return EXTRA_VALUE1; });
            map.computeIfAbsent("lol", k -> { ctr2.incrementAndGet(); assertEquals(k, "lol"); return null; });
            assertEquals(ref0.get(), ctr0.get());
            assertEquals(ref1.get(), ctr1.get());
            assertEquals(ref2.get(), ctr2.get());
            assertMapEq(reference, map);
        }
    }

    @RepeatedTest(5)
    @Order(31)
    public void testComputeIfPresent() {
        final AtomicReference<String> val0 = new AtomicReference<>(), val1 = new AtomicReference<>(), val2 = new AtomicReference<>();
        final AtomicInteger ref0 = new AtomicInteger(), ref1 = new AtomicInteger(), ref2 = new AtomicInteger();
        final AtomicInteger ctr0 = new AtomicInteger(), ctr1 = new AtomicInteger(), ctr2 = new AtomicInteger();
        final Map<String, String> reference = new HashMap<String, String>(REFERENCE);
        reference.computeIfPresent("bar", (k,v) -> { ref0.incrementAndGet(); val0.set(v); return EXTRA_VALUE0; });
        reference.computeIfPresent("xyz", (k,v) -> { ref1.incrementAndGet(); val1.set(v); return EXTRA_VALUE1; });
        reference.computeIfPresent("baz", (k,v) -> { ref2.incrementAndGet(); val2.set(v); return null; });
        try(final SQLiteMap<String, String> map = SQLiteMap.fromMemory(String.class, String.class)) {
            for (final Entry<String, String> ref : REFERENCE.entrySet()) {
                map.put0(ref.getKey(), ref.getValue());
            }
            map.computeIfPresent("bar", (k,v) -> { ctr0.incrementAndGet(); assertEquals(k, "bar"); assertEquals(v, val0.get()); return EXTRA_VALUE0; });
            map.computeIfPresent("xyz", (k,v) -> { ctr1.incrementAndGet(); assertEquals(k, "xyz"); assertEquals(v, val1.get()); return EXTRA_VALUE1; });
            map.computeIfPresent("baz", (k,v) -> { ctr2.incrementAndGet(); assertEquals(k, "baz"); assertEquals(v, val2.get()); return null; });
            assertEquals(ref0.get(), ctr0.get());
            assertEquals(ref1.get(), ctr1.get());
            assertEquals(ref2.get(), ctr2.get());
            assertMapEq(reference, map);
        }
    }

    @RepeatedTest(5)
    @Order(32)
    public void testClear() {
        final Map<String, String> reference = new HashMap<String, String>(REFERENCE);
        reference.clear();
        reference.put("xyz", EXTRA_VALUE0);
        reference.put("lol", EXTRA_VALUE1);
        try(final SQLiteMap<String, String> map = SQLiteMap.fromMemory(String.class, String.class)) {
            for (final Entry<String, String> ref : REFERENCE.entrySet()) {
                map.put0(ref.getKey(), ref.getValue());
            }
            assertMapEq(REFERENCE, map);
            map.clear();
            assertMapEq(Collections.emptyMap(), map);
            map.put0("xyz", EXTRA_VALUE0);
            map.put0("lol", EXTRA_VALUE1);
            assertMapEq(reference, map);
        }
    }

    @RepeatedTest(5)
    @Order(33)
    public void testSize() {
        final int expected = REFERENCE.size();
        try(final SQLiteMap<String, String> map = SQLiteMap.fromMemory(String.class, String.class)) {
            for (final Entry<String, String> ref : REFERENCE.entrySet()) {
                map.put0(ref.getKey(), ref.getValue());
            }
            assertEquals(expected, map.size());
            assertEquals(expected, map.sizeLong());
            map.put("bar", EXTRA_VALUE0);
            assertEquals(expected, map.size());
            assertEquals(expected, map.sizeLong());
            map.put("xyz", EXTRA_VALUE1);
            assertEquals(expected + 1, map.size());
            assertEquals(expected + 1, map.sizeLong());
            map.clear();
            assertEquals(0, map.size());
            assertEquals(0, map.sizeLong());
        }
    }

    @RepeatedTest(5)
    @Order(34)
    public void testIsEmpty() {
        try(final SQLiteMap<String, String> map = SQLiteMap.fromMemory(String.class, String.class)) {
            assertTrue(map.isEmpty());
            for (final Entry<String, String> ref : REFERENCE.entrySet()) {
                map.put0(ref.getKey(), ref.getValue());
            }
            assertFalse(map.isEmpty());
            map.clear();
            assertTrue (map.isEmpty());
        }
    }

    @RepeatedTest(5)
    @Order(35)
    public void testIterator() {
        try(final SQLiteMap<String, String> map = SQLiteMap.fromMemory(String.class, String.class)) {
            assertTrue(map.isEmpty());
            for (final Entry<String, String> ref : REFERENCE.entrySet()) {
                map.put0(ref.getKey(), ref.getValue());
            }
            final Map<String, String> entries = new LinkedHashMap<String, String>();
            try (final SQLiteMap<String, String>.SQLiteMapEntryIterator iter = map.iterator()) {
                while (iter.hasNext()) {
                    final Entry<String, String> current = iter.next();
                    entries.put(current.getKey(), current.getValue());
                }
            }
            assertEquals(REFERENCE, entries);
        }
    }

    @RepeatedTest(5)
    @Order(36)
    public void testKeyIterator() {
        try(final SQLiteMap<String, String> map = SQLiteMap.fromMemory(String.class, String.class)) {
            assertTrue(map.isEmpty());
            for (final Entry<String, String> ref : REFERENCE.entrySet()) {
                map.put0(ref.getKey(), ref.getValue());
            }
            final Set<String> keys = new LinkedHashSet<String>();
            try (final SQLiteMap<String, String>.SQLiteMapKeyIterator iter = map.keyIterator()) {
                while (iter.hasNext()) {
                    keys.add(iter.next());
                }
            }
            assertCollectionEq(REFERENCE.keySet(), keys);
        }
    }

    @RepeatedTest(5)
    @Order(37)
    public void testValueIterator() {
        try(final SQLiteMap<String, String> map = SQLiteMap.fromMemory(String.class, String.class)) {
            for (final Entry<String, String> ref : REFERENCE.entrySet()) {
                map.put0(ref.getKey(), ref.getValue());
            }
            final Set<String> values = new LinkedHashSet<String>();
            try (final SQLiteMap<String, String>.SQLiteMapValueIterator iter = map.valueIterator()) {
                while (iter.hasNext()) {
                    values.add(iter.next());
                }
            }
            assertCollectionEq(REFERENCE.values(), values);
        }
    }

    @RepeatedTest(5)
    @Order(38)
    public void testKeySet() {
        try(final SQLiteMap<String, String> map = SQLiteMap.fromMemory(String.class, String.class)) {
            for (final Entry<String, String> ref : REFERENCE.entrySet()) {
                map.put0(ref.getKey(), ref.getValue());
            }
            final SQLiteMap<String, String>.SQLiteMapKeySet keySet = map.keySet();
            assertEquals(REFERENCE.size(), keySet.size());
            assertEquals(REFERENCE.size(), keySet.sizeLong());
            assertCollectionEq(REFERENCE.keySet(), keySet);
            assertTrue(keySet.containsAll(REFERENCE.keySet()));
            try (final SQLiteMap<String, String>.SQLiteMapKeyIterator iter = map.keyIterator()) {
                while (iter.hasNext()) {
                    final String key = iter.next();
                    keySet.contains(key);
                }
            }
            assertFalse(keySet.isEmpty());
            keySet.clear();
            assertTrue (keySet.isEmpty());
        }
    }

    @RepeatedTest(5)
    @Order(39)
    public void testValues() {
        try(final SQLiteMap<String, String> map = SQLiteMap.fromMemory(String.class, String.class)) {
            for (final Entry<String, String> ref : REFERENCE.entrySet()) {
                map.put0(ref.getKey(), ref.getValue());
            }
            final SQLiteMap<String, String>.SQLiteMapValueCollection values = map.values();
            assertEquals(REFERENCE.size(), values.size());
            assertEquals(REFERENCE.size(), values.sizeLong());
            assertCollectionEq(REFERENCE.values(), values);
            assertTrue(values.containsAll(REFERENCE.values()));
            try (final SQLiteMap<String, String>.SQLiteMapValueIterator iter = map.valueIterator()) {
                while (iter.hasNext()) {
                    final String key = iter.next();
                    values.contains(key);
                }
            }
            assertFalse(values.isEmpty());
            values.clear();
            assertTrue (values.isEmpty());
        }
    }

    @RepeatedTest(5)
    @Order(40)
    public void testEntrySet() {
        try(final SQLiteMap<String, String> map = SQLiteMap.fromMemory(String.class, String.class)) {
            for (final Entry<String, String> ref : REFERENCE.entrySet()) {
                map.put0(ref.getKey(), ref.getValue());
            }
            final Set<String> keys = new LinkedHashSet<String>();
            final SQLiteMap<String, String>.SQLiteMapEntrySet entries = map.entrySet();
            assertEquals(REFERENCE.size(), entries.size());
            assertEquals(REFERENCE.size(), entries.sizeLong());
            for (final Entry<String, String> entry : entries) {
                final String key = entry.getKey();
                assertNotNull(key);
                final String value  = REFERENCE.get(key);
                assertNotNull(value);
                assertEquals(value, entry.getValue());
                keys.add(key);
            }
            assertCollectionEq(REFERENCE.keySet(), keys);
            assertFalse(entries.isEmpty());
            entries.clear();
            assertTrue (entries.isEmpty());
        }
    }

    @RepeatedTest(5)
    @Order(41)
    public void testHashCode() {
        try(final SQLiteMap<String, String> map = SQLiteMap.fromMemory(String.class, String.class)) {
            for (final Entry<String, String> ref : REFERENCE.entrySet()) {
                map.put0(ref.getKey(), ref.getValue());
            }
            final int hash0a = map.hashCode();
            final int hash0b = map.hashCode();
            map.put("xyz", EXTRA_VALUE0);
            final int hash1a = map.hashCode();
            final int hash1b = map.hashCode();
            map.clear();
            final int hash2a = map.hashCode();
            final int hash2b = map.hashCode();
            assertEquals(hash0a, hash0b);
            assertEquals(hash1a, hash1b);
            assertEquals(hash2a, hash2b);
            assertNotEquals(hash0a, hash1a);
            assertNotEquals(hash1a, hash2a);
            assertNotEquals(hash2a, hash0a);
        }
    }

    @RepeatedTest(5)
    @Order(42)
    public void testEquals() {
        final Map<String, String> reference = new HashMap<String, String>(REFERENCE);
        try(final SQLiteMap<String, String> map = SQLiteMap.fromMemory(String.class, String.class)) {
            for (final Entry<String, String> ref : REFERENCE.entrySet()) {
                map.put0(ref.getKey(), ref.getValue());
            }
            assertTrue (map.equals(map));
            assertFalse(map.equals((Object)Integer.valueOf(42)));
            assertTrue (map.equals(reference));
            map.put0("xzy", EXTRA_VALUE0);
            assertFalse(map.equals(reference));
            reference.put("xzy", EXTRA_VALUE0);
            assertTrue (map.equals(reference));
            map.remove("bar");
            assertFalse(map.equals(reference));
            reference.remove("bar");
            assertTrue (map.equals(reference));
        }
    }

    @RepeatedTest(5)
    @Order(43)
    public void testForEach() {
        final Map<String, String> entries = new HashMap<String, String>();
        try(final SQLiteMap<String, String> map = SQLiteMap.fromMemory(String.class, String.class)) {
            for (final Entry<String, String> ref : REFERENCE.entrySet()) {
                map.put0(ref.getKey(), ref.getValue());
            }
            map.forEach((k, v) -> entries.put(k, v));
        }
        assertEquals(REFERENCE, entries);
    }

    @RepeatedTest(5)
    @Order(44)
    public void testSetValueIndex() throws Exception {
        try(final SQLiteMap<String, String> map = SQLiteMap.fromMemory(String.class, String.class)) {
            map.setValueIndexEnabled(true);
            for (final Entry<String, String> ref : REFERENCE.entrySet()) {
                map.put0(ref.getKey(), ref.getValue());
            }
            map.setValueIndexEnabled(false);
        }
        try(final SQLiteMap<String, String> map = SQLiteMap.fromMemory(String.class, String.class)) {
            map.setValueIndexEnabled(false);
            for (final Entry<String, String> ref : REFERENCE.entrySet()) {
                map.put0(ref.getKey(), ref.getValue());
            }
            map.setValueIndexEnabled(true);
        }
    }

    @RepeatedTest(5)
    @Order(45)
    public void testClose() throws Exception {
        final SQLiteMap<String, String> map = SQLiteMap.fromMemory(String.class, String.class);
        map.close();
        assertThrows(IllegalStateException.class, () -> map.put("xyz", EXTRA_VALUE0));
    }

    @RepeatedTest(5)
    @Order(46)
    public void testRandomStrings() throws Exception {
        final SecureRandom secureRandom = new SecureRandom();
        final SecretKey desKey0 = new SecretKeySpec(secureRandom.generateSeed(Long.BYTES), "DES");
        final SecretKey desKey1 = new SecretKeySpec(secureRandom.generateSeed(Long.BYTES), "DES");
        final Cipher cipher0 = Cipher.getInstance("DES/ECB/NoPadding");
        final Cipher cipher1 = Cipher.getInstance("DES/ECB/NoPadding");
        try(final SQLiteMap<String, String> map = SQLiteMap.fromMemory(String.class, String.class)) {
            final int COUNT = 0x10000;
            cipher0.init(Cipher.ENCRYPT_MODE, desKey0);
            cipher1.init(Cipher.ENCRYPT_MODE, desKey1);
            for (int i = 0; i < COUNT; ++i) {
                final long key = cryptToLong(cipher0, i), value = cryptToLong(cipher1, i);
                System.out.printf("%04X -> %016X -> %016X%n", i, key, value);
                map.put0(String.format("%016X", key), String.format("%016X", value));
            }
            cipher1.init(Cipher.DECRYPT_MODE, desKey1);
            final int[] indices = shuffle(IntStream.range(0, COUNT).toArray(), secureRandom);
            for (final int i : indices) {
                final long key = cryptToLong(cipher0, i);
                final String valueEncr = map.get(String.format("%016X", key));
                assertNotNull(valueEncr);
                final long value = cryptToLong(cipher1, Long.parseUnsignedLong(valueEncr, 16));
                assertEquals(i, value);
                System.out.printf("%016X -> %s -> %04X%n", key, valueEncr, value);
            }
        }
    }

    @RepeatedTest(5)
    @Order(47)
    public void testRandomBytes() throws Exception {
        final SecureRandom secureRandom = new SecureRandom();
        final SecretKey desKey0 = new SecretKeySpec(secureRandom.generateSeed(Long.BYTES), "DES");
        final SecretKey desKey1 = new SecretKeySpec(secureRandom.generateSeed(Long.BYTES), "DES");
        final Cipher cipher0 = Cipher.getInstance("DES/ECB/NoPadding");
        final Cipher cipher1 = Cipher.getInstance("DES/ECB/NoPadding");
        try(final SQLiteMap<byte[], byte[]> map = SQLiteMap.fromMemory(byte[].class, byte[].class)) {
            final int COUNT = 0x10000;
            cipher0.init(Cipher.ENCRYPT_MODE, desKey0);
            cipher1.init(Cipher.ENCRYPT_MODE, desKey1);
            for (int i = 0; i < COUNT; ++i) {
                final byte[] key = cryptToBytes(cipher0, i), value = cryptToBytes(cipher1, i);
                System.out.printf("%04X -> %s -> %s%n", i, bytesToHexStr(key), bytesToHexStr(value));
                map.put0(key, value);
            }
            cipher1.init(Cipher.DECRYPT_MODE, desKey1);
            final int[] indices = shuffle(IntStream.range(0, COUNT).toArray(), secureRandom);
            for (final int i : indices) {
                final byte[] key = cryptToBytes(cipher0, i);
                final byte[] valueEncr = map.get(key);
                assertNotNull(valueEncr);
                final long value = cryptToLong(cipher1, valueEncr);
                assertEquals(i, value);
                System.out.printf("%s -> %s -> %04X%n", bytesToHexStr(key), bytesToHexStr(valueEncr), value);
            }
        }
    }

    // ======================================================================
    // Utility Methods
    // ======================================================================

    private static <K, V> void assertMapEq(final Map<K, V> reference, final SQLiteMap<K, V> map) {
        final Set<K> allKeys = new LinkedHashSet<K>();
        try (final SQLiteMap<K, V>.SQLiteMapEntryIterator iter = map.iterator()) {
            while (iter.hasNext()) {
                final Entry<K, V> item = iter.next();
                final K key = item.getKey();
                assertTrue(reference.containsKey(key));
                final V expected = reference.get(key);
                assertEquals(expected, item.getValue());
                allKeys.add(key);
                System.out.printf("%s -> %s%n", item.getKey(), item.getValue());
            }
        }
        System.out.println();
        assertCollectionEq(reference.keySet(), allKeys);
    }
}
