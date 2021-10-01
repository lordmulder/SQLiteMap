package com.muldersoft.maps.sqlite.test;

import java.security.SecureRandom;
import java.util.Objects;

import com.muldersoft.maps.sqlite.SQLiteMap;

public class Main {

    private static final int UPDATE_INTERVAL = 1000;
    private static final int LIMIT = 100000000;

    private static final char[] HEX_CHARS = "0123456789ABCDEF".toCharArray();

    public static void main(String[] args)  throws Exception {
        final SecureRandom random = new SecureRandom();
        final byte[] key = new byte[32], message = new byte[32];
        printMemoryStats();
        try (final SQLiteMap<String, String> map = SQLiteMap.fromMemory(String.class, String.class)) {
            try {
                long currentTime = -1L, nextUpdate = System.currentTimeMillis() + UPDATE_INTERVAL;
                byte spinner0 = 0, spinner1 = 0;
                for (int counter = 0; counter < LIMIT; ++counter) {
                    random.nextBytes(key);
                    random.nextBytes(message);
                    map.put0(bytesToHex(key), bytesToHex(message));
                    if ((++spinner0 == 0) && ((currentTime = System.currentTimeMillis()) >= nextUpdate)) {
                        System.out.printf("%,d%n", map.size());
                        if (++spinner1 >= 5) {
                            spinner1 = 0;
                            printMemoryStats();
                        }
                        nextUpdate = currentTime + UPDATE_INTERVAL;
                    }
                }
            } finally {
                System.out.println("Completed!");
                System.out.printf("%,d%n", map.size());
                System.gc();
                printMemoryStats();
            }
        }
        Thread.sleep(Long.MAX_VALUE);
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
}
