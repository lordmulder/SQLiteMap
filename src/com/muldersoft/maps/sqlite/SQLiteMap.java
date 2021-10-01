package com.muldersoft.maps.sqlite;

import java.io.Closeable;
import java.math.BigInteger;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.regex.Pattern;

/**
 * <h1>SQLiteMap - a map implementation for Java based on SQLite</h1>
 * <p>
 * This work is licensed under the
 * <a href="https://creativecommons.org/publicdomain/zero/1.0/legalcode">CC0 1.0 Universal License</a>.
 * <p>
 * The {@code SQLiteMap} class provides a map implementation that is backed by an SQLite database. It can employ an "in-memory"
 * database or a local database file. Compared to Java's standard {@code HashMap} class, the "in-memory" variant of
 * {@code SQLiteMap} is better suited for <i>very large</i> maps; it has a smaller memory footprint and does <b>not</b> clutter
 * the Java heap space. The file-based variant of {@code SQLiteMap} can handle even bigger maps and provides full persistence.
 * <p>
 * This class is <b>not</b> "thread-safe", in the sense that the <i>same</i> instance of {@code SQLiteMap} <b>must not</b> be
 * accessed concurrently by <i>different</i> threads. However, it is perfectly "safe" to created <i>multiple</i> instances of
 * {@code SQLiteMap} in <i>different</i> threads; each instance uses its own separate SQLite connection and its own separate
 * database table. As long as each thread <i>only</i> accesses its own instance an does <i>not</i> share that instance with
 * other threads, <b>no</b> synchronization is required. In case that the <i>same</i> instance of {@code SQLiteMap} needs to be
 * shared across <i>different</i> threads, the application <b>must</b> explicitly <i>synchronize</i> <b>all</b> accesses to
 * that "shared" instance! This includes any iterators, key/entry sets or value collections returned by this class.
 * <p>
 * <b>Important notice:</b> Instances of this class <i>must</i> explicitly be {@link close}'d when they are no longer needed!
 *
 * @author LoRd_MuldeR (mulder2@gmx.de) | <a href="http://muldersoft.com/">muldersoft.com</a>
 * @param <K> the type of keys maintained by this map
 * @param <V> the type of mapped values
 */
public final class SQLiteMap<K,V> implements Map<K,V>, Iterable<Map.Entry<K, V>>, AutoCloseable {

    private static final short VERSION_MAJOR = 1;
    private static final short VERSION_MINOR = 0;
    private static final short VERSION_PATCH = 0;

    /**
     * Returns the version of the {@code SQLiteMap} library
     * @return the {@code short[]} array, containing the major, minor and patch version
     */
    public static short[] getVersion() {
        return new short[] { VERSION_MAJOR, VERSION_MINOR, VERSION_PATCH };
    }

    // ======================================================================
    // SQL Statements
    // ======================================================================

    private static final Pattern REGEX_TABLE_NAME = Pattern.compile("[\\w\\.$@#]+");

    private static final String SQL_CREATE_TABLE  = "CREATE TABLE IF NOT EXISTS `%s` (key %s CONSTRAINT pk_key PRIMARY KEY, value %s NOT NULL);"; // WITHOUT ROWID
    private static final String SQL_DESTROY_TABLE = "DROP TABLE `%s`;";
    private static final String SQL_CREATE_INDEX  = "CREATE INDEX IF NOT EXISTS `%1$s.idx_value` ON `%1$s` (value);";
    private static final String SQL_DESTROY_INDEX = "DROP INDEX IF EXISTS `%s.idx_value`;";
    private static final String SQL_FETCH_ENTRY   = "SELECT value FROM `%s` WHERE key = ?;";
    private static final String SQL_FETCH_KEYS    = "SELECT key FROM `%s`;";
    private static final String SQL_FETCH_VALUES  = "SELECT value FROM `%s`;";
    private static final String SQL_FETCH_ENTRIES = "SELECT key, value FROM `%s`;";
    private static final String SQL_INSERT_ENTRY  = "INSERT INTO `%s` (key, value) VALUES (?, ?);";
    private static final String SQL_INSERT_ENTRY0 = "INSERT INTO `%s` (key, value) VALUES (?, ?) ON CONFLICT(key) DO NOTHING;";
    private static final String SQL_UPSERT_ENTRY  = "INSERT INTO `%s` (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value;";
    private static final String SQL_UPDATE_ENTRY  = "UPDATE `%s` SET value = ? WHERE key = ?;";
    private static final String SQL_REMOVE_ENTRY  = "DELETE FROM `%s` WHERE key = ?;";
    private static final String SQL_CLEAR_ENTRIES = "DELETE FROM `%s`;";
    private static final String SQL_COUNT_ENTRIES = "SELECT COUNT(*) FROM `%s`;";
    private static final String SQL_COUNT_KEYS    = "SELECT COUNT(*) FROM `%s` WHERE key = ?;";
    private static final String SQL_COUNT_VALUES  = "SELECT COUNT(*) FROM `%s` WHERE value = ? LIMIT 1;";

    private class SQLStatements implements Closeable {
        final PreparedStatement fetchEntry;
        final PreparedStatement fetchKeys;
        final PreparedStatement fetchValues;
        final PreparedStatement fetchEntries;
        final PreparedStatement insertEntry;
        final PreparedStatement insertEntry0;
        final PreparedStatement upsertEntry;
        final PreparedStatement updateEntry;
        final PreparedStatement removeEntry;
        final PreparedStatement clearEntries;
        final PreparedStatement countEntries;
        final PreparedStatement countKeys;
        final PreparedStatement countValues;

        public SQLStatements() throws SQLException {
            fetchEntry   = connection.prepareStatement(String.format(SQL_FETCH_ENTRY,   tableName));
            fetchKeys    = connection.prepareStatement(String.format(SQL_FETCH_KEYS,    tableName));
            fetchValues  = connection.prepareStatement(String.format(SQL_FETCH_VALUES,  tableName));
            fetchEntries = connection.prepareStatement(String.format(SQL_FETCH_ENTRIES, tableName));
            insertEntry  = connection.prepareStatement(String.format(SQL_INSERT_ENTRY,  tableName));
            insertEntry0 = connection.prepareStatement(String.format(SQL_INSERT_ENTRY0, tableName));
            upsertEntry  = connection.prepareStatement(String.format(SQL_UPSERT_ENTRY,  tableName));
            updateEntry  = connection.prepareStatement(String.format(SQL_UPDATE_ENTRY,  tableName));
            removeEntry  = connection.prepareStatement(String.format(SQL_REMOVE_ENTRY,  tableName));
            clearEntries = connection.prepareStatement(String.format(SQL_CLEAR_ENTRIES, tableName));
            countEntries = connection.prepareStatement(String.format(SQL_COUNT_ENTRIES, tableName));
            countKeys    = connection.prepareStatement(String.format(SQL_COUNT_KEYS,    tableName));
            countValues  = connection.prepareStatement(String.format(SQL_COUNT_VALUES,  tableName));
        }

        @Override
        public void close() {
            try {
                fetchEntry.close();
                fetchKeys.close();
                fetchValues.close();
                fetchEntries.close();
                insertEntry.close();
                insertEntry0.close();
                upsertEntry.close();
                updateEntry.close();
                removeEntry.close();
                clearEntries.close();
                countEntries.close();
                countKeys.close();
                countValues.close();
            } catch (final SQLException e) {
                throw new SQLiteMapException("Failed to close SQL statements!", e);
            }
        }
    }

    // ======================================================================
    // Instance Variables
    // ======================================================================

    private final Type<K> typeK;
    private final Type<V> typeV;

    private final boolean isTemporary;
    private final String tableName;
    private final Connection connection;
    private final SQLStatements sql;

    private SQLiteMapEntrySet entrySet = null;
    private SQLiteMapKeySet keySet = null;
    private SQLiteMapValueCollection valueCollection = null;

    private long generation = 0L;
    private boolean isClosed = false;

    // ======================================================================
    // Exception
    // ======================================================================

    /**
     * Exception class to indicate {@link SQLiteMap} errors
     */
    public static class SQLiteMapException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        public SQLiteMapException(final String message) {
            super(message);
        }

        public SQLiteMapException(final String message, final Throwable cause) {
            super(message, cause);
        }
    }

    // ======================================================================
    // Types
    // ======================================================================

    private static final List<Type<?>> TYPES = Collections.unmodifiableList(Arrays.asList(
        new BytesType(),
        new StringType(),
        new IntegerType(),
        new LongType(),
        new DateTimeType(),
        new BigIntegerType()));

    private static abstract class Type<T> {
        protected final String TYPE_TEXT    = "TEXT";
        protected final String TYPE_INTEGER = "INTEGER";
        protected final String TYPE_BLOB    = "BLOB";

        protected final int ID_INTEGER =  4;
        protected final int ID_VARCHAR = 12;

        protected final Class<T> clazz;

        protected Type(final Class<T> clazz) {
            this.clazz = Objects.requireNonNull(clazz);
        }

        public Class<T> getNativeType() {
            return clazz;
        }

        public abstract T fromObject(final Object value);
        public abstract void setParameter(final PreparedStatement statement, final int index, final T value) throws SQLException;
        public abstract T getResult(final ResultSet result, final int index) throws SQLException;
        public abstract boolean isEmpty(final T value);
        public abstract boolean equals(final T value0, final T value1);
        public abstract String typeName();
        public abstract int storageTypeId();
    }

    private static class BytesType extends Type<byte[]> {
        public BytesType() {
            super(byte[].class);
        }

        @Override
        public byte[] fromObject(final Object value) {
            try {
                return (byte[]) value;
            } catch(final ClassCastException e) {
                throw new IllegalArgumentException("Argument has an invalid type. Only byte[] is allowed!", e);
            }
        }

        @Override
        public boolean isEmpty(final byte[] value) {
            return (value == null) || (value.length == 0);
        }

        @Override
        public void setParameter(final PreparedStatement statement, final int index, final byte[] value) throws SQLException {
            statement.setBytes(index, value);
        }

        @Override
        public byte[] getResult(final ResultSet result, final int index) throws SQLException {
            return result.getBytes(index);
        }

        @Override
        public boolean equals(final byte[] value0, final byte[] value1) {
            return Arrays.equals(value0, value1);
        }

        @Override
        public String typeName() {
            return TYPE_BLOB;
        }

        @Override
        public int storageTypeId() {
            return ID_VARCHAR;
        }
    }

    private static class StringType extends Type<String> {
        public StringType() {
            super(String.class);
        }

        @Override
        public String fromObject(final Object value) {
            try {
                return (String) value;
            } catch(final ClassCastException e) {
                throw new IllegalArgumentException("Argument has an invalid type. Only String is allowed!", e);
            }
        }

        @Override
        public boolean isEmpty(final String value) {
            return (value == null) || (value.isEmpty());
        }

        @Override
        public void setParameter(final PreparedStatement statement, final int index, final String value) throws SQLException {
            statement.setString(index, value);
        }

        @Override
        public String getResult(final ResultSet result, final int index) throws SQLException {
            return result.getString(index);
        }

        @Override
        public boolean equals(final String value0, final String value1) {
            if (value0 == value1) {
                return true;
            } else if ((value0 == null) || (value1 == null)) {
                return false;
            }
            return value0.equals(value1);
        }

        @Override
        public String typeName() {
            return TYPE_TEXT;
        }

        @Override
        public int storageTypeId() {
            return ID_VARCHAR;
        }
    }

    private static class IntegerType extends Type<Integer> {
        public IntegerType() {
            super(Integer.class);
        }

        @Override
        public Integer fromObject(final Object value) {
            try {
                return (Integer) value;
            } catch(final ClassCastException e) {
                throw new IllegalArgumentException("Argument has an invalid type. Only Integer is allowed!", e);
            }
        }

        @Override
        public boolean isEmpty(final Integer value) {
            return (value == null);
        }

        @Override
        public void setParameter(final PreparedStatement statement, final int index, final Integer value) throws SQLException {
            statement.setInt(index, value);
        }

        @Override
        public Integer getResult(final ResultSet result, final int index) throws SQLException {
            return result.getInt(index);
        }

        @Override
        public boolean equals(final Integer value0, final Integer value1) {
            if (value0 == value1) {
                return true;
            } else if ((value0 == null) || (value1 == null)) {
                return false;
            }
            return value0.equals(value1);
        }

        @Override
        public String typeName() {
            return TYPE_INTEGER;
        }

        @Override
        public int storageTypeId() {
            return ID_INTEGER;
        }
    }

    private static class LongType extends Type<Long> {
        public LongType() {
            super(Long.class);
        }

        @Override
        public Long fromObject(final Object value) {
            try {
                return (Long) value;
            } catch(final ClassCastException e) {
                throw new IllegalArgumentException("Argument has an invalid type. Only Long is allowed!", e);
            }
        }

        @Override
        public boolean isEmpty(final Long value) {
            return (value == null);
        }

        @Override
        public void setParameter(final PreparedStatement statement, final int index, final Long value) throws SQLException {
            statement.setLong(index, value);
        }

        @Override
        public Long getResult(final ResultSet result, final int index) throws SQLException {
            return result.getLong(index);
        }

        @Override
        public boolean equals(final Long value0, final Long value1) {
            if (value0 == value1) {
                return true;
            } else if ((value0 == null) || (value1 == null)) {
                return false;
            }
            return value0.equals(value1);
        }

        @Override
        public String typeName() {
            return TYPE_INTEGER;
        }

        @Override
        public int storageTypeId() {
            return ID_INTEGER;
        }
    }

    private static class DateTimeType extends Type<Instant> {
        public DateTimeType() {
            super(Instant.class);
        }

        @Override
        public Instant fromObject(final Object value) {
            try {
                return (Instant) value;
            } catch(final ClassCastException e) {
                throw new IllegalArgumentException("Argument has an invalid type. Only Instant is allowed!", e);
            }
        }

        @Override
        public boolean isEmpty(final Instant value) {
            return (value == null);
        }

        @Override
        public void setParameter(final PreparedStatement statement, final int index, final Instant value) throws SQLException {
            statement.setTimestamp(index, Timestamp.from(value));
        }

        @Override
        public Instant getResult(final ResultSet result, final int index) throws SQLException {
            return result.getTimestamp(index).toInstant();
        }

        @Override
        public boolean equals(final Instant value0, final Instant value1) {
            if (value0 == value1) {
                return true;
            } else if ((value0 == null) || (value1 == null)) {
                return false;
            }
            return value0.equals(value1);
        }

        @Override
        public String typeName() {
            return TYPE_INTEGER;
        }

        @Override
        public int storageTypeId() {
            return ID_INTEGER;
        }
    }

    private static class BigIntegerType extends Type<BigInteger> {
        public BigIntegerType() {
            super(BigInteger.class);
        }

        @Override
        public BigInteger fromObject(final Object value) {
            try {
                return (BigInteger) value;
            } catch(final ClassCastException e) {
                throw new IllegalArgumentException("Argument has an invalid type. Only Instant is allowed!", e);
            }
        }

        @Override
        public boolean isEmpty(final BigInteger value) {
            return (value == null);
        }

        @Override
        public void setParameter(final PreparedStatement statement, final int index, final BigInteger value) throws SQLException {
            statement.setBytes(index, value.toByteArray());
        }

        @Override
        public BigInteger getResult(final ResultSet result, final int index) throws SQLException {
            return new BigInteger(result.getBytes(index));
        }

        @Override
        public boolean equals(final BigInteger value0, final BigInteger value1) {
            if (value0 == value1) {
                return true;
            } else if ((value0 == null) || (value1 == null)) {
                return false;
            }
            return value0.equals(value1);
        }

        @Override
        public String typeName() {
            return TYPE_BLOB;
        }

        @Override
        public int storageTypeId() {
            return ID_VARCHAR;
        }

    }

    // ======================================================================
    // Map Entry
    // ======================================================================

    public class SQLiteMapEntry implements Entry<K, V> {
        private final K key;
        private final V value;

        private SQLiteMapEntry(final K key, final V value) {
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

        @Override
        public V setValue(V value) {
            throw new UnsupportedOperationException();
        }
    }

    // ======================================================================
    // Map Iterator
    // ======================================================================

    public abstract class SQLiteMapAbstractIterator<T> implements Iterator<T>, AutoCloseable {
        protected final ResultSet result;
        protected final long generation;
        protected int state = 0;

        protected SQLiteMapAbstractIterator(final ResultSet result, final long generation) {
            this.generation = generation;
            this.result = Objects.requireNonNull(result);
        }

        @Override
        public boolean hasNext() {
            if (state < 0) {
                return false;
            } else if (state > 0) {
                return true;
            } else {
                return fetchNextElement();
            }
        }

        protected boolean fetchNextElement() {
            if (state < 0) {
                throw new IllegalStateException("Already at end of result set!");
            }
            try {
                ensureResultIsStillValid();
                if (result.next()) {
                    state = 1;
                    return true;
                } else {
                    close(); /*no more elements!*/
                    return false;
                }
            } catch (final SQLException e) {
                throw new SQLiteMapException("Failed to query next element!", e);
            }
        }

        protected void ensureResultIsStillValid() throws SQLException {
            if ((generation != SQLiteMap.this.generation) || result.isClosed()) {
                throw new IllegalStateException("Iterator invalidated!");
            }
        }

        @Override
        public void close() {
            if (state >= 0) {
                state = -1;
                try {
                    result.close();
                } catch (final SQLException e) {
                    throw new SQLiteMapException("Failed to clean up iterator!", e);
                }
            }
        }
    }

    public class SQLiteMapKeyIterator extends SQLiteMapAbstractIterator<K> {
        private SQLiteMapKeyIterator(final ResultSet result, final long generation) {
            super(result, generation);
        }

        @Override
        public K next() {
            if ((state < 0) || ((state == 0) && (!fetchNextElement()))) {
                throw new NoSuchElementException("No more elements are available!");
            }
            try {
                ensureResultIsStillValid();
                return typeK.getResult(result, 1);
            } catch (final SQLException e) {
                throw new SQLiteMapException("Failed to read next key-value pair!", e);
            } finally {
                state = 0; /*reset state!*/
            }
        }
    }

    public class SQLiteMapValueIterator extends SQLiteMapAbstractIterator<V> {
        private SQLiteMapValueIterator(final ResultSet result, final long generation) {
            super(result, generation);
        }

        @Override
        public V next() {
            if ((state < 0) || ((state == 0) && (!fetchNextElement()))) {
                throw new NoSuchElementException("No more elements are available!");
            }
            try {
                ensureResultIsStillValid();
                return typeV.getResult(result, 1);
            } catch (final SQLException e) {
                throw new SQLiteMapException("Failed to read next key-value pair!", e);
            } finally {
                state = 0; /*reset state!*/
            }
        }
    }

    public class SQLiteMapEntryIterator extends SQLiteMapAbstractIterator<Entry<K, V>> {
        private SQLiteMapEntryIterator(final ResultSet result, final long generation) {
            super(result, generation);
        }

        @Override
        public Entry<K, V> next() {
            if ((state < 0) || ((state == 0) && (!fetchNextElement()))) {
                throw new NoSuchElementException("No more elements are available!");
            }
            try {
                ensureResultIsStillValid();
                return new SQLiteMapEntry(typeK.getResult(result, 1), typeV.getResult(result, 2));
            } catch (final SQLException e) {
                throw new SQLiteMapException("Failed to read next key-value pair!", e);
            } finally {
                state = 0; /*reset state!*/
            }
        }
    }

    // ======================================================================
    // Entry Set
    // ======================================================================

    public abstract class SQLiteMapAbstractSet<T> implements Set<T> {
        @Override
        public int size() {
            return SQLiteMap.this.size();
        }

        @Override
        public boolean isEmpty() {
            return SQLiteMap.this.isEmpty();
        }

        @Override
        public Object[] toArray() {
            throw new UnsupportedOperationException();
        }

        @Override
        public <U> U[] toArray(U[] a) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean add(T e) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean remove(Object o) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean containsAll(Collection<?> c) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean addAll(Collection<? extends T> c) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean retainAll(Collection<?> c) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean removeAll(Collection<?> c) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void clear() {
            throw new UnsupportedOperationException();
        }
    }

    public class SQLiteMapKeySet extends SQLiteMapAbstractSet<K> {
        @Override
        public boolean contains(final Object o) {
            return SQLiteMap.this.containsKey(o);
        }

        @Override
        public Iterator<K> iterator() {
            return SQLiteMap.this.keyIterator();
        }
    }

    public class SQLiteMapValueCollection extends SQLiteMapAbstractSet<V> {
        @Override
        public boolean contains(final Object o) {
            return SQLiteMap.this.containsValue(o);
        }

        @Override
        public Iterator<V> iterator() {
            return SQLiteMap.this.valueIterator();
        }
    }

    public class SQLiteMapEntrySet extends SQLiteMapAbstractSet<Entry<K, V>> {
        @Override
        public boolean contains(final Object o) {
            final Entry<K, V> entry = toMapEntry(o);
            if (entry != null) {
                return typeV.equals(entry.getValue(), SQLiteMap.this.get(entry.getKey()));
            }
            return false;
        }

        @Override
        public Iterator<Entry<K, V>> iterator() {
            return SQLiteMap.this.iterator();
        }

        @SuppressWarnings("unchecked")
        private Entry<K, V> toMapEntry(final Object o) {
            try {
                return (Entry<K, V>) o;
            } catch (final ClassCastException e) {
                return null;
            }
        }
    }

    // ======================================================================
    // Constructor
    // ======================================================================

    static {
        final String SQLITE_DRIVER = "org.sqlite.JDBC";
        try {
            Class.forName(SQLITE_DRIVER);
        } catch (final ClassNotFoundException e) {
            throw new RuntimeException("Failed to initialize SQLite JDBC driver!");
        }
    }

    private SQLiteMap(final Type<K> typeK, final Type<V> typeV) {
        this(typeK, typeV, null, null, true);
    }

    private SQLiteMap(final Type<K> typeK, final Type<V> typeV, final Path path, final String tableName) {
        this(typeK, typeV, path, tableName, false);
    }

    private SQLiteMap(final Type<K> typeK, final Type<V> typeV, final Path path, final String tableName, final boolean temporary) {
        this.typeK = Objects.requireNonNull(typeK, "typeK");
        this.typeV = Objects.requireNonNull(typeV, "typeV");
        this.isTemporary = temporary;
        if ((tableName != null) && (!REGEX_TABLE_NAME.matcher(tableName).matches())) {
            throw new IllegalArgumentException("Illgeal table name!");
        }
        this.tableName = (tableName != null) ? tableName : String.format("%s$%08X", getClass().getCanonicalName(), System.identityHashCode(this));
        try {
            connection = DriverManager.getConnection(String.format("jdbc:sqlite:%s", (path != null) ? path.toString() : ":memory:"));
            if (path != null) {
                try (final Statement statement = connection.createStatement()) {
                    statement.executeUpdate("PRAGMA journal_mode=WAL;");
                }
            }
        } catch (final SQLException e) {
            throw new SQLiteMapException("Failed to create SQLite connection!", e);
        }
        try {
            createSQLiteTable();
        } catch (final SQLException e) {
            throw new SQLiteMapException("Failed to create database table!", e);
        }
        try {
            sql = new SQLStatements();
        } catch (SQLException e) {
            throw new SQLiteMapException("Failed to initialize SQL statements!", e);
        }
    }

    /**
     * Creates a new {@code SQLiteMap} instance that is backed by an in-memory database
     * @param keyType the type of the keys stored in the map
     * @param valueType the type of the values stored in the map
     * @return the new {@code SQLiteMap} instance
     */
    public static <K,V> SQLiteMap<K,V> fromMemory(final Class<K> keyType, final Class<V> valueType) {
        return new SQLiteMap<K, V>(typeOf(keyType), typeOf(valueType));
    }

    /**
     * Creates a new {@code SQLiteMap} instance that is backed by a local database file
     * @param keyType the type of the keys stored in the map
     * @param valueType the type of the values stored in the map
     * @param path the path of the database file that will be used to store the map
     * @param tableName the name of the database table that will be used to store key-value pairs
     * @return the new SQLiteMap instance
     * @implNote the {@code SQLiteMap} returned by this method does <b>not</b> drop the table
     */
    public static <K,V> SQLiteMap<K,V> fromFile(final Class<K> keyType, final Class<V> valueType, final Path path, final String tableName) {
        return fromFile(keyType, valueType, path, tableName, false);
    }

    /**
     * Creates a new {@code SQLiteMap} instance that is backed by a local database file
     * @param keyType the type of the keys stored in the map
     * @param valueType the type of the values stored in the map
     * @param path the path of the database file that will be used to store the map
     * @param tableName the name of the database table that will be used to store key-value pairs
     * @param temporary if {@code true}, the table will be dropped when the map is closed
     * @return the new {@code SQLiteMap} instance
     */
    public static <K,V> SQLiteMap<K,V> fromFile(final Class<K> keyType, final Class<V> valueType, final Path path, final String tableName, final boolean temporary) {
        Objects.requireNonNull(path, "Path must not be null!");
        Objects.requireNonNull(tableName, "Table name must not be null!");
        return new SQLiteMap<K, V>(typeOf(keyType), typeOf(valueType), path, tableName, temporary);
    }

    // ======================================================================
    // Public Methods
    // ======================================================================

    @Override
    public boolean containsKey(final Object key) {
        final K typedKey = typeK.fromObject(key);
        if (typeK.isEmpty(typedKey)) {
            return false;
        }
        ensureConnectionNotClosed();
        try {
            typeK.setParameter(sql.countKeys, 1, typedKey);
            try (final ResultSet result = sql.countKeys.executeQuery()) {
                if (result.next()) {
                    return result.getLong(1) > 0;
                }
                return false;
            }
        } catch(final SQLException e) {
            throw new SQLiteMapException("Failed to query the number of key-value pairs!", e);
        }
    }

    @Override
    public boolean containsValue(final Object value) {
        final V typedValue = typeV.fromObject(value);
        if (typeV.isEmpty(typedValue)) {
            return false;
        }
        ensureConnectionNotClosed();
        try {
            typeV.setParameter(sql.countValues, 1, typedValue);
            try (final ResultSet result = sql.countValues.executeQuery()) {
                if (result.next()) {
                    return result.getLong(1) > 0;
                }
                return false;
            }
        } catch(final SQLException e) {
            throw new SQLiteMapException("Failed to query the number of key-value pairs!", e);
        }
    }

    @Override
    public V get(final Object key) {
        final K typedKey = typeK.fromObject(key);
        if (typeK.isEmpty(typedKey)) {
            return null;
        }
        ensureConnectionNotClosed();
        try {
            return fetchEntry(typedKey);
        } catch (final Exception e) {
            throw new SQLiteMapException("Failed to query existing key-value pair!", e);
        }
    }

    @Override
    public V getOrDefault(final Object key, final V defaultValue) {
        final K typedKey = typeK.fromObject(key);
        if (typeK.isEmpty(typedKey)) {
            return defaultValue;
        }
        ensureConnectionNotClosed();
        try {
            final V value = fetchEntry(typedKey);
            return (value != null) ? value : defaultValue;
        } catch (final Exception e) {
            throw new SQLiteMapException("Failed to query existing key-value pair!", e);
        }
    }

    @Override
    public V put(final K key, final V value) {
        if (typeK.isEmpty(key)) {
            throw new IllegalArgumentException("Key must not be null or empty!");
        }
        if (typeV.isEmpty(value)) {
            throw new IllegalArgumentException("Value must not be null or empty!");
        }
        ensureConnectionNotClosed();
        try {
            return transaction(connection, () -> {
                final V previousValue = fetchEntry(key);
                upsertEntry(key, value);
                return previousValue;
            });
        } catch (final Exception e) {
            throw new SQLiteMapException("Failed to store new key-value pair!", e);
        }
    }

    /**
     * A version of {@link put} that does <b>not</b> return the previous value (performance optimization)
     * @param key
     * @param value
     */
    public void put0(final K key, final V value) {
        if (typeK.isEmpty(key)) {
            throw new IllegalArgumentException("Key must not be null or empty!");
        }
        if (typeV.isEmpty(value)) {
            throw new IllegalArgumentException("Value must not be null or empty!");
        }
        ensureConnectionNotClosed();
        try {
            upsertEntry(key, value);
        } catch (final Exception e) {
            throw new SQLiteMapException("Failed to store new key-value pair!", e);
        }
    }

    @Override
    public V putIfAbsent(final K key, final V value) {
        if (typeK.isEmpty(key)) {
            throw new IllegalArgumentException("Key must not be null or empty!");
        }
        if (typeV.isEmpty(value)) {
            throw new IllegalArgumentException("Value must not be null or empty!");
        }
        ensureConnectionNotClosed();
        try {
            return transaction(connection, () -> {
                final V previousValue = fetchEntry(key);
                if (previousValue == null) {
                    insertEntry(key, value);
                }
                return previousValue;
            });
        } catch (final Exception e) {
            throw new SQLiteMapException("Failed to store new key-value pair!", e);
        }
    }

    /**
     * A version of {@link putIfAbsent} that does <b>not</b> return the previous value (performance optimization)
     * @param key
     * @param value
     */
    public void putIfAbsent0(final K key, final V value) {
        if (typeK.isEmpty(key)) {
            throw new IllegalArgumentException("Key must not be null or empty!");
        }
        if (typeV.isEmpty(value)) {
            throw new IllegalArgumentException("Value must not be null or empty!");
        }
        ensureConnectionNotClosed();
        try {
            insertEntryOptional(key, value);
        } catch (final Exception e) {
            throw new SQLiteMapException("Failed to store new key-value pair!", e);
        }
    }

    @Override
    public void putAll(final Map<? extends K, ? extends V> map) {
        if (map == null) {
            throw new IllegalArgumentException("Map must not be null!");
        }
        ensureConnectionNotClosed();
        try {
            transaction(connection, () -> {
                for(final Entry<? extends K, ? extends V> entry : map.entrySet()) {
                    final K key = entry.getKey();
                    final V value = entry.getValue();
                    if (typeK.isEmpty(key)) {
                        throw new IllegalArgumentException("Key must not be null or empty!");
                    }
                    if (typeV.isEmpty(value)) {
                        throw new IllegalArgumentException("Value must not be null or empty!");
                    }
                    upsertEntry(key, value);
                }
                return null;
            });
        } catch (final Exception e) {
            throw new SQLiteMapException("Failed to store new key-value pair!", e);
        }
    }

     @Override
    public V compute(final K key, final BiFunction<? super K,? super V,? extends V> remappingFunction) {
        if (typeK.isEmpty(key)) {
            throw new IllegalArgumentException("Key must not be null or empty!");
        }
        if (remappingFunction == null) {
            throw new IllegalArgumentException("Re-mapping function must not be null or empty!");
        }
        ensureConnectionNotClosed();
        try {
            return transaction(connection, () -> {
                final V previousValue = fetchEntry(key);
                final V value = remappingFunction.apply(key, previousValue);
                if (typeV.isEmpty(value)) {
                    if (previousValue != null) {
                        removeEntry(key);
                    }
                } else {
                    upsertEntry(key, value);
                }
                return value;
            });
        } catch (final Exception e) {
            throw new SQLiteMapException("Failed to store new key-value pair!", e);
        }
    }

    @Override
    public V computeIfAbsent(final K key, final Function<? super K,? extends V> mappingFunction) {
        if (typeK.isEmpty(key)) {
            throw new IllegalArgumentException("Key must not be null or empty!");
        }
        if (mappingFunction == null) {
            throw new IllegalArgumentException("Mapping function must not be null or empty!");
        }
        ensureConnectionNotClosed();
        try {
            return transaction(connection, () -> {
                V value;
                if ((value = fetchEntry(key)) == null) {
                    if (!typeV.isEmpty(value = mappingFunction.apply(key))) {
                        insertEntry(key, value);
                    }
                }
                return value;
            });
        } catch (final Exception e) {
            throw new SQLiteMapException("Failed to store new key-value pair!", e);
        }
    }

    @Override
    public V computeIfPresent(final K key, final BiFunction<? super K,? super V,? extends V> remappingFunction) {
        if (typeK.isEmpty(key)) {
            throw new IllegalArgumentException("Key must not be null or empty!");
        }
        if (remappingFunction == null) {
            throw new IllegalArgumentException("Re-mapping function must not be null or empty!");
        }
        ensureConnectionNotClosed();
        try {
            return transaction(connection, () -> {
                V value;
                if ((value = fetchEntry(key)) != null) {
                    if (!typeV.isEmpty(value = remappingFunction.apply(key, value))) {
                        updateEntry(key, value);
                    } else {
                        removeEntry(key);
                    }
                }
                return value;
            });
        } catch (final Exception e) {
            throw new SQLiteMapException("Failed to store new key-value pair!", e);
        }
    }

    @Override
    public V replace(final K key, final V value) {
        if (typeK.isEmpty(key)) {
            return null;
        }
        ensureConnectionNotClosed();
        try {
            return transaction(connection, () -> {
                final V previousValue = fetchEntry(key);
                if (previousValue != null) {
                    if (typeV.isEmpty(value)) {
                        removeEntry(key);
                    } else {
                        updateEntry(key, value);
                    }
                }
                return previousValue;
            });
        } catch (final Exception e) {
            throw new SQLiteMapException("Failed to store new key-value pair!", e);
        }
    }

    @Override
    public V remove(Object key) {
        final K typedKey = typeK.fromObject(key);
        if (typeK.isEmpty(typedKey)) {
            return null;
        }
        ensureConnectionNotClosed();
        try {
            return transaction(connection, () -> {
                final V previousValue = fetchEntry(typedKey);
                if (previousValue != null) {
                    removeEntry(typedKey);
                }
                return previousValue;
            });
        } catch (final Exception e) {
            throw new SQLiteMapException("Failed to remove key-value pair!", e);
        }
    }

    @Override
    public void clear() {
        ensureConnectionNotClosed();
        ++generation;
        try {
            sql.clearEntries.executeUpdate();
        } catch(final SQLException e) {
            throw new SQLiteMapException("Failed to query the number of key-value pairs!", e);
        }
    }

    @Override
    public boolean isEmpty() {
        return (!(sizeLong() > 0L));
    }

    @Override
    public int size() {
        return Math.toIntExact(sizeLong());
    }

    @Override
    public Set<K> keySet() {
        if (keySet == null) {
            keySet = new SQLiteMapKeySet();
        }
        return keySet;
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        if (entrySet == null) {
            entrySet = new SQLiteMapEntrySet();
        }
        return entrySet;
    }

    @Override
    public Collection<V> values() {
        if (valueCollection == null) {
            valueCollection = new SQLiteMapValueCollection();
        }
        return valueCollection;
    }

    @Override
    public void forEach(final BiConsumer<? super K, ? super V> action) {
        Objects.requireNonNull(action);
        try (final SQLiteMapEntryIterator iterator = iterator()) {
            while(iterator.hasNext()) {
                final Entry<K, V> entry = iterator.next();
                action.accept(entry.getKey(), entry.getValue());
            }
        }
    }

    /**
     * A version of {@link size} that returns the number key-value pairs in this map as a <b>long</b> value
     * @return the number of key-value pairs in this map
     */
    public long sizeLong() {
        ensureConnectionNotClosed();
        try {
            try (final ResultSet result = sql.countEntries.executeQuery()) {
                if (result.next()) {
                    return result.getLong(1);
                }
                return -1L;
            }
        } catch(final SQLException e) {
            throw new SQLiteMapException("Failed to query the number of key-value pairs!", e);
        }
    }

    /**
     * Returns an iterator over the key-value pairs in this map. The key-value pairs are returned in no particular order.
     * @return an iterator over the key-value pairs in this map
     */
    @Override
    public SQLiteMapEntryIterator iterator() {
        ensureConnectionNotClosed();
        try {
            return new SQLiteMapEntryIterator(sql.fetchEntries.executeQuery(), ++generation);
        } catch(final SQLException e) {
            throw new SQLiteMapException("Failed to query the existing key-value pairs!", e);
        }
    }

    /**
     * Returns an iterator over the keys in this map. The keys are returned in no particular order.
     * @return an iterator over the keys in this map
     */
    public SQLiteMapKeyIterator keyIterator() {
        ensureConnectionNotClosed();
        try {
            return new SQLiteMapKeyIterator(sql.fetchKeys.executeQuery(), ++generation);
        } catch(final SQLException e) {
            throw new SQLiteMapException("Failed to query the existing key set!", e);
        }
    }

    /**
     * Returns an iterator over the values in this map. The values are returned in no particular order.
     * @return an iterator over the values in this map
     */
    public SQLiteMapValueIterator valueIterator() {
        ensureConnectionNotClosed();
        try {
            return new SQLiteMapValueIterator(sql.fetchValues.executeQuery(), ++generation);
        } catch(final SQLException e) {
            throw new SQLiteMapException("Failed to query the existing value set!", e);
        }
    }

    /**
     * Create an index on the "value" column or drop the existing index.
     * <p>
     * An index on the "value" column can significantly speed up <b>value</b> <i>lookup</i> operations, but it comes at a
     * certain memory overhead and may slow down <i>insert</i> operations. Initially, there is <b>no</b> "value" index.
     * @param enable if {@code true} creates the index, otherwise drops the index
     */
    public void setValueIndexEnabled(final boolean enable) {
        ensureConnectionNotClosed();
        try (final Statement statement = connection.createStatement()) {
            statement.executeUpdate(String.format(enable ? SQL_CREATE_INDEX : SQL_DESTROY_INDEX, tableName));
        } catch(final SQLException e) {
            throw new SQLiteMapException("Failed to create or drop the index!", e);
        }
    }

    /**
     * Close the underlying database connection and free all resources associated with this map. This method <b>must</b> be
     * called when this {@code SQLiteMap} is no longer needed; otherwise a resource leak will occur!
     */
    @Override
    public void close() {
        if (!isClosed) {
            isClosed = true;
            try {
                try {
                    if (isTemporary) {
                        try (final Statement statement = connection.createStatement()) {
                            statement.executeUpdate(String.format(SQL_DESTROY_TABLE, tableName));
                        }
                    }
                    sql.close();
                } finally {
                    connection.close();
                }
            } catch (final SQLException e) {
                throw new SQLiteMapException("Failed to close SQLite connection!", e);
            }
        }
    }

    // --------------------------------------------------------
    // Unsupported Operations (for now)
    // --------------------------------------------------------

    /**
     * Not currently implemented!
     * @exception UnsupportedOperationException
     */
    @Override
    public boolean remove(Object key, Object value) {
        throw new UnsupportedOperationException();
    }

    /**
     * Not currently implemented!
     * @exception UnsupportedOperationException
     */
    @Override
    public void replaceAll(BiFunction<? super K, ? super V, ? extends V> function) {
        throw new UnsupportedOperationException();
    }

    /**
     * Not currently implemented!
     * @exception UnsupportedOperationException
     */
    @Override
    public boolean replace(K key, V oldValue, V newValue) {
        throw new UnsupportedOperationException();
    }

    /**
     * Not currently implemented!
     * @exception UnsupportedOperationException
     */
    @Override
    public V merge(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction) {
        throw new UnsupportedOperationException();
    }

    // ======================================================================
    // Private Methods
    // ======================================================================

    private void ensureConnectionNotClosed() {
        if (isClosed) {
            throw new IllegalStateException("Connection has already been closed!");
        }
    }

    private void createSQLiteTable() throws SQLException {
        try (final PreparedStatement statement = connection.prepareStatement(String.format(SQL_CREATE_TABLE, tableName, typeK.typeName(), typeV.typeName()))) {
            statement.executeUpdate();
        }
        try (final ResultSet result = connection.getMetaData().getColumns(null, null, tableName, null)) {
            int columnCount = 0;
            try {
                while(result.next()) {
                    switch(++columnCount) {
                    case 1:
                        verifyProperty(result, "COLUMN_NAME", "key");
                        verifyProperty(result, "DATA_TYPE", String.valueOf(typeK.storageTypeId()));
                        break;
                    case 2:
                        verifyProperty(result, "COLUMN_NAME", "value");
                        verifyProperty(result, "DATA_TYPE", String.valueOf(typeV.storageTypeId()));
                        break;
                    }
                }
                if (columnCount != 2) {
                    throw new AssertionError("Invalid number of columns detected! [columnCount=" + columnCount + "]");
                }
            } catch (final AssertionError e) {
                throw new SQLiteMapException("Table meta data differes from expected definition!", e);
            }
        }
    }

    private V fetchEntry(final K key) {
        try  {
            typeK.setParameter(sql.fetchEntry, 1, key);
            try (final ResultSet result = sql.fetchEntry.executeQuery()) {
                if (result.next()) {
                    return typeV.getResult(result, 1);
                }
            }
        } catch(final SQLException e) {
            throw new SQLiteMapException("Failed to query the existing value!", e);
        }
        return null;
    }

    private void insertEntry(final K key, final V value) {
        ++generation;
        try {
            typeK.setParameter(sql.insertEntry, 1, key);
            typeV.setParameter(sql.insertEntry, 2, value);
            sql.insertEntry.executeUpdate();
        } catch (final SQLException e) {
            throw new SQLiteMapException("Failed to insert the new key-value pair!", e);
        }
    }

    private void insertEntryOptional(final K key, final V value) {
        ++generation;
        try {
            typeK.setParameter(sql.insertEntry0, 1, key);
            typeV.setParameter(sql.insertEntry0, 2, value);
            sql.insertEntry0.executeUpdate();
        } catch (final SQLException e) {
            throw new SQLiteMapException("Failed to insert the new key-value pair!", e);
        }
    }

    private void upsertEntry(final K key, final V value) {
        ++generation;
        try {
            typeK.setParameter(sql.upsertEntry, 1, key);
            typeV.setParameter(sql.upsertEntry, 2, value);
            sql.upsertEntry.executeUpdate();
        } catch (final SQLException e) {
            throw new SQLiteMapException("Failed to upsert the new key-value pair!", e);
        }
    }

    private void updateEntry(final K key, final V value) {
        ++generation;
        try {
            typeK.setParameter(sql.updateEntry, 2, key);
            typeV.setParameter(sql.updateEntry, 1, value);
            sql.updateEntry.executeUpdate();
        } catch (final SQLException e) {
            throw new SQLiteMapException("Failed to insert the new key-value pair!", e);
        }
    }

    private void removeEntry(final K key) {
        ++generation;
        try {
            typeK.setParameter(sql.removeEntry, 1, key);
            sql.removeEntry.executeUpdate();
        } catch (final SQLException e) {
            throw new SQLiteMapException("Failed to remove the key-value pair!", e);
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> Type<T> typeOf(final Class<T> clazz) {
        Objects.requireNonNull(clazz, "Class parameter must not be null!");
        for (final Type<?> type : TYPES) {
            if (type.getNativeType().equals(clazz)) {
                return (Type<T>) type;
            }
        }
        throw new IllegalArgumentException("Unsupported type: " + clazz.getName());
    }

    private static void verifyProperty(final ResultSet result, final String property, final String expected) throws SQLException {
        final String value = result.getString(property);
        if (!expected.equals(value)) {
            throw new AssertionError("Mismatch in property \"" + property + "\" detected! [expected \"" + expected + "\", but was \"" + value + "\"]");
        }
    }

    private static <T> T transaction(final Connection connection, final Callable<T> callable) throws SQLException {
        final T result;
        try {
            connection.setAutoCommit(false);
            result = callable.call();
            connection.commit();
        } catch (final Exception e) {
            try {
                connection.rollback(); /*something went wrong!*/
            } catch (final SQLException e2) {
                throw new Error("The rollback has failed!", e2);
            }
            throw new SQLException("The transaction has failed!", e);
        } finally {
            try {
                connection.setAutoCommit(true);
            } catch (SQLException e) { }
        }
        return result;
    }
}
