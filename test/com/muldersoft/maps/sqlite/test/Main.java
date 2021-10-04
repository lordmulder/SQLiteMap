package com.muldersoft.maps.sqlite.test;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;

import com.muldersoft.maps.sqlite.SQLiteMap;

public class Main {

    private static final int LIMIT = 100000000;
    private static final int UPDATE_INTERVAL = 1000;
    private static final int BATCH_SIZE = 256;

    private static final char[] HEX_CHARS = "0123456789ABCDEF".toCharArray();

    public static void main(String[] args)  throws Exception {
        final ThreadLocalRandom random = ThreadLocalRandom.current();
        final byte[] buffer = new byte[32];
        final MutableEntry<String, String>[] data = allocateArray(BATCH_SIZE);
        printMemoryStats();
        try (final SQLiteMap<String, String> map = SQLiteMap.fromMemory(String.class, String.class)) {
            try {
                final List<MutableEntry<String, String>> list = Arrays.asList(data);
                long currentTime = -1L, nextUpdate = System.currentTimeMillis() + UPDATE_INTERVAL;
                final long startTime = System.currentTimeMillis();
                for (int counter = 0; counter < LIMIT; counter += data.length) {
                    for (int i = 0; i < data.length; ++i) {
                        random.nextBytes(buffer);
                        data[i].setKey(bytesToHex(buffer));
                        random.nextBytes(buffer);
                        data[i].setValue(bytesToHex(buffer));
                    }
                    map.putAll(list);
                    if ((currentTime = System.currentTimeMillis()) >= nextUpdate) {
                        System.out.printf("%,d%n", map.size());
                        nextUpdate = currentTime + UPDATE_INTERVAL;
                    }
                }
                final long endTime = System.currentTimeMillis();
                System.out.printf("Total time: %,d%n", endTime - startTime);
            } finally {
                System.out.println("Completed!");
                System.out.printf("%,d%n", map.size());
                printMemoryStats();
                System.gc();
                printMemoryStats();
                Thread.sleep(9999);
            }
        }
    }

    private static void printMemoryStats() {
        final Runtime runtime = Runtime.getRuntime();
        System.out.printf("Memory: free=%,d, total=%,d, max=%,d%n", runtime.freeMemory(), runtime.totalMemory(), runtime.maxMemory());
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
    private static <K, V> MutableEntry<K, V>[] allocateArray(final int length) {
        final MutableEntry<K, V>[] array = (MutableEntry<K, V>[]) Array.newInstance(MutableEntry.class, length);
        for (int i = 0; i < length; ++i) {
            array[i] = new MutableEntry<K, V>();
        }
        return array;
    }
}
