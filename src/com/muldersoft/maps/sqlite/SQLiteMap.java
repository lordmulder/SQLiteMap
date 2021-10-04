package com.muldersoft.maps.sqlite;

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
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.Deque;
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
import java.util.stream.Collectors;

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

    private static final int MAXIMUM_BATCH_SIZE = 256;

    // ======================================================================
    // Instance Variables
    // ======================================================================

    private final Type<K> typeK;
    private final Type<V> typeV;

    private final Connection connection;
    private final String tableName;
    private final boolean isTemporary;

    private SQLiteMapEntrySet entrySet = null;
    private SQLiteMapKeySet keySet = null;
    private SQLiteMapValueCollection valueCollection = null;

    private long generation = 0L, hashCodeGen = -1L;
    private int hashCode = 0;
    private boolean isClosed = false;

    private Deque<AutoCloseable> cleanUpQueue = new ArrayDeque<AutoCloseable>();

    // ======================================================================
    // SQL Statements
    // ======================================================================

    private static final Pattern REGEX_TABLE_NAME = Pattern.compile("[\\w\\.!$@#]+");

    private static final String SQL_CREATE_TABLE  = "CREATE TABLE IF NOT EXISTS `%s` (key %s CONSTRAINT pk_key PRIMARY KEY, value %s NOT NULL);";
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

    private final PreparedStatementHolder sqlFetchEntry   = new PreparedStatementHolder(SQL_FETCH_ENTRY);
    private final PreparedStatementHolder sqlFetchKeys    = new PreparedStatementHolder(SQL_FETCH_KEYS);
    private final PreparedStatementHolder sqlFetchValues  = new PreparedStatementHolder(SQL_FETCH_VALUES);
    private final PreparedStatementHolder sqlFetchEntries = new PreparedStatementHolder(SQL_FETCH_ENTRIES);
    private final PreparedStatementHolder sqlInsertEntry  = new PreparedStatementHolder(SQL_INSERT_ENTRY);
    private final PreparedStatementHolder sqlInsertEntry0 = new PreparedStatementHolder(SQL_INSERT_ENTRY0);
    private final PreparedStatementHolder sqlUpsertEntry  = new PreparedStatementHolder(SQL_UPSERT_ENTRY);
    private final PreparedStatementHolder sqlUpdateEntry  = new PreparedStatementHolder(SQL_UPDATE_ENTRY);
    private final PreparedStatementHolder sqlRemoveEntry  = new PreparedStatementHolder(SQL_REMOVE_ENTRY);
    private final PreparedStatementHolder sqlClearEntries = new PreparedStatementHolder(SQL_CLEAR_ENTRIES);
    private final PreparedStatementHolder sqlCountEntries = new PreparedStatementHolder(SQL_COUNT_ENTRIES);
    private final PreparedStatementHolder sqlCountKeys    = new PreparedStatementHolder(SQL_COUNT_KEYS);
    private final PreparedStatementHolder sqlCountValues  = new PreparedStatementHolder(SQL_COUNT_VALUES);
    private final PreparedStatementHolder sqlCreateIndex  = new PreparedStatementHolder(SQL_CREATE_INDEX);
    private final PreparedStatementHolder sqlDestroyIndex = new PreparedStatementHolder(SQL_DESTROY_INDEX);

    private class PreparedStatementHolder implements AutoCloseable {
        private final String sql;
        private PreparedStatement statement;

        public PreparedStatementHolder(final String sql) {
            this.sql = Objects.requireNonNull(sql);
        }

        public PreparedStatement getInstance() {
            if (statement == null) {
                try {
                    statement = connection.prepareStatement(String.format(sql, tableName));
                } catch (final Exception e) {
                    throw new SQLiteMapException("Failed to prepare SQL statement!", e);
                }
                cleanUpQueue.addLast(this);
            }
            return statement;
        }

        @Override
        public void close() {
            try {
                if (statement != null) {
                    statement.close();
                }
                statement = null;
            } catch (final SQLException e) {
                throw new SQLiteMapException("Failed to close SQL statement!", e);
            } finally {
                cleanUpQueue.remove(this);
            }
        }
    }

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
        new BooleanType(),
        new ByteType(),
        new BytesType(),
        new StringType(),
        new IntegerType(),
        new LongType(),
        new InstantType(),
        new BigIntegerType()));

    private static final Map<Class<?>, Type<?>> TYPE_MAP = Collections.unmodifiableMap(TYPES.stream()
        .collect(Collectors.toMap(t -> t.getUnderlyingType(), t -> t)));

    private static abstract class Type<T> {
        protected final String typeName;
        protected final int typeId;
        protected final Class<T> underlyingType;

        protected static final String TYPE_TEXT    = "TEXT";
        protected static final String TYPE_INTEGER = "INTEGER";
        protected static final String TYPE_BLOB    = "BLOB";

        protected static final int ID_INTEGER =  4;
        protected static final int ID_VARCHAR = 12;

        public Type(final Class<T> underlyingType, final String typeName, final int typeId) {
            this.typeName = Objects.requireNonNull(typeName);
            this.typeId = typeId;
            this.underlyingType = Objects.requireNonNull(underlyingType);
        }

        public Class<T> getUnderlyingType() {
            return underlyingType;
        }

        public String typeName() {
            return typeName;
        }

        public int getTypeId() {
            return typeId;
        }

        public int hashCode(final T value) {
            if (value != null) {
                return value.hashCode();
            }
            return 0;
        }

        public boolean equals(final T value0, final Object value1) {
            if (value0 == value1) {
                return true;
            } else if ((value0 == null) || (value1 == null)) {
                return false;
            } else {
                return value0.equals(value1);
            }
        }

        public abstract T fromObject(final Object value);
        public abstract void setParameter(final PreparedStatement statement, final int index, final T value) throws SQLException;
        public abstract T getResult(final ResultSet result, final int index) throws SQLException;
    }

    private static class BooleanType extends Type<Boolean> {
        public BooleanType() {
            super(Boolean.class, TYPE_INTEGER, ID_INTEGER);
        }

        @Override
        public Boolean fromObject(final Object value) {
            return (value instanceof Boolean) ? ((Boolean)value) : null;
        }

        @Override
        public void setParameter(final PreparedStatement statement, final int index, final Boolean value) throws SQLException {
            statement.setBoolean(index, value);
        }

        @Override
        public Boolean getResult(final ResultSet result, final int index) throws SQLException {
            return result.getBoolean(index);
        }
    }

    private static class ByteType extends Type<Byte> {
        public ByteType() {
            super(Byte.class, TYPE_INTEGER, ID_INTEGER);
        }

        @Override
        public Byte fromObject(final Object value) {
            return (value instanceof Byte) ? ((Byte)value) : null;
        }

        @Override
        public void setParameter(final PreparedStatement statement, final int index, final Byte value) throws SQLException {
            statement.setByte(index, value);
        }

        @Override
        public Byte getResult(final ResultSet result, final int index) throws SQLException {
            return result.getByte(index);
        }
    }

    private static class BytesType extends Type<byte[]> {
        public BytesType() {
            super(byte[].class, TYPE_BLOB, ID_VARCHAR);
        }

        @Override
        public byte[] fromObject(final Object value) {
            return (value instanceof byte[]) ? ((byte[])value) : null;
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
        public int hashCode(final byte[] value) {
            return Arrays.hashCode(value);
        }

        @Override
        public boolean equals(final byte[] value0, final Object value1) {
            if (value0 == value1) {
                return true;
            } else if (value1 instanceof byte[]) {
                return Arrays.equals(value0, (byte[])value1);
            } else {
                return false;
            }
        }
    }

    private static class StringType extends Type<String> {
        public StringType() {
            super(String.class, TYPE_TEXT, ID_VARCHAR);
        }

        @Override
        public String fromObject(final Object value) {
            return (value instanceof String) ? ((String)value) : null;
        }

        @Override
        public void setParameter(final PreparedStatement statement, final int index, final String value) throws SQLException {
            statement.setString(index, value);
        }

        @Override
        public String getResult(final ResultSet result, final int index) throws SQLException {
            return result.getString(index);
        }
    }

    private static class IntegerType extends Type<Integer> {
        public IntegerType() {
            super(Integer.class, TYPE_INTEGER, ID_INTEGER);
        }

        @Override
        public Integer fromObject(final Object value) {
            return (value instanceof Integer) ? ((Integer)value) : null;
        }

        @Override
        public void setParameter(final PreparedStatement statement, final int index, final Integer value) throws SQLException {
            statement.setInt(index, value);
        }

        @Override
        public Integer getResult(final ResultSet result, final int index) throws SQLException {
            return result.getInt(index);
        }
    }

    private static class LongType extends Type<Long> {
        public LongType() {
            super(Long.class, TYPE_INTEGER, ID_INTEGER);
        }

        @Override
        public Long fromObject(final Object value) {
            return (value instanceof Long) ? ((Long)value) : null;
        }

        @Override
        public void setParameter(final PreparedStatement statement, final int index, final Long value) throws SQLException {
            statement.setLong(index, value);
        }

        @Override
        public Long getResult(final ResultSet result, final int index) throws SQLException {
            return result.getLong(index);
        }
    }

    private static class InstantType extends Type<Instant> {
        public InstantType() {
            super(Instant.class, TYPE_INTEGER, ID_INTEGER);
        }

        @Override
        public Instant fromObject(final Object value) {
            return (value instanceof Instant) ? ((Instant)value) : null;
        }

        @Override
        public void setParameter(final PreparedStatement statement, final int index, final Instant value) throws SQLException {
            statement.setTimestamp(index, Timestamp.from(value));
        }

        @Override
        public Instant getResult(final ResultSet result, final int index) throws SQLException {
            return result.getTimestamp(index).toInstant();
        }
    }

    private static class BigIntegerType extends Type<BigInteger> {
        public BigIntegerType() {
            super(BigInteger.class, TYPE_BLOB, ID_VARCHAR);
        }

        @Override
        public BigInteger fromObject(final Object value) {
            return (value instanceof BigInteger) ? ((BigInteger)value) : null;
        }

        @Override
        public void setParameter(final PreparedStatement statement, final int index, final BigInteger value) throws SQLException {
            statement.setBytes(index, value.toByteArray());
        }

        @Override
        public BigInteger getResult(final ResultSet result, final int index) throws SQLException {
            return new BigInteger(result.getBytes(index));
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

        @Override
        public int hashCode() {
            return (65599 * typeK.hashCode(key)) + typeV.hashCode(value);
        }

        @Override
        public boolean equals(final Object o) {
            if (o == this) {
                return true;
            } else if (o instanceof Entry) {
                final Entry<?, ?> entry = (Entry<?, ?>)o;
                return typeK.equals(key, entry.getKey()) && typeV.equals(value, entry.getValue());
            } else {
                return false;
            }
        }
    }

    // ======================================================================
    // Map Iterator
    // ======================================================================

    public abstract class SQLiteMapAbstractIterator<T> implements Iterator<T>, AutoCloseable {
        protected final long generation;
        protected final ResultSet resultSet;
        protected int state = 0;

        protected SQLiteMapAbstractIterator(final ResultSet resultSet, final long generation) {
            this.generation = generation;
            this.resultSet = Objects.requireNonNull(resultSet);
            cleanUpQueue.addLast(this);
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
                ensureResultSetIsStillValid();
                if (resultSet.next()) {
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

        protected void ensureResultSetIsStillValid() throws SQLException {
            if (generation != SQLiteMap.this.generation) {
                try {
                    close();
                } catch (Exception e) { }
                throw new ConcurrentModificationException("The map has been modified after this iterator was created!");
            }
        }

        @Override
        public void close() {
            if (state >= 0) {
                state = -1;
                try {
                    resultSet.close();
                } catch (final SQLException e) {
                    throw new SQLiteMapException("Failed to clean up iterator!", e);
                } finally {
                    cleanUpQueue.remove(this);
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
                ensureResultSetIsStillValid();
                return typeK.getResult(resultSet, 1);
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
                throw new NoSuchElementException("No further elements are available!");
            }
            try {
                ensureResultSetIsStillValid();
                return typeV.getResult(resultSet, 1);
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
                ensureResultSetIsStillValid();
                return new SQLiteMapEntry(typeK.getResult(resultSet, 1), typeV.getResult(resultSet, 2));
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

        public long sizeLong() {
            return SQLiteMap.this.sizeLong();
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
        public SQLiteMapKeyIterator iterator() {
            return SQLiteMap.this.keyIterator();
        }
    }

    public class SQLiteMapValueCollection extends SQLiteMapAbstractSet<V> {
        @Override
        public boolean contains(final Object o) {
            return SQLiteMap.this.containsValue(o);
        }

        @Override
        public SQLiteMapValueIterator iterator() {
            return SQLiteMap.this.valueIterator();
        }
    }

    public class SQLiteMapEntrySet extends SQLiteMapAbstractSet<Entry<K, V>> {
        @Override
        public boolean contains(final Object o) {
            if (o instanceof Entry) {
                final Entry<?, ?> entry = (Entry<?, ?>) o;
                final V value = SQLiteMap.this.get(entry.getKey());
                if (value != null) {
                    return typeV.equals(value, entry.getValue());
                }
            }
            return false;
        }

        @Override
        public SQLiteMapEntryIterator iterator() {
            return SQLiteMap.this.iterator();
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
        this.tableName = (tableName != null) ? tableName : String.format("%s$%08X", getClass().getName(), System.identityHashCode(this));
        createSQLiteTable();
    }

    /**
     * Creates a new {@code SQLiteMap} instance that is backed by an in-memory database.
     * <p>
     * The {@code SQLiteMap} returned by this method <i>does</i> drop the table when calling {@link close}.
     *
     * @param keyType the type of the keys stored in the map
     * @param valueType the type of the values stored in the map
     * @return the new {@code SQLiteMap} instance
     */
    public static <K,V> SQLiteMap<K,V> fromMemory(final Class<K> keyType, final Class<V> valueType) {
        return new SQLiteMap<K, V>(typeOf(keyType), typeOf(valueType));
    }

    /**
     * Creates a new {@code SQLiteMap} instance that is backed by a local database file.
     * <p>
     * The {@code SQLiteMap} returned by this method does <b>not</b> drop the table when calling {@link close}.
     *
     * @param keyType the type of the keys stored in the map
     * @param valueType the type of the values stored in the map
     * @param path the path of the database file that will be used to store the map
     * @param tableName the name of the database table that will be used to store key-value pairs
     * @return the new SQLiteMap instance
     */
    public static <K,V> SQLiteMap<K,V> fromFile(final Class<K> keyType, final Class<V> valueType, final Path path, final String tableName) {
        return fromFile(keyType, valueType, path, tableName, false);
    }

    /**
     * Creates a new {@code SQLiteMap} instance that is backed by a local database file.
     * <p>
     * The {@code SQLiteMap} returned by this method <i>optionally</i> drops the table when calling {@link close}.
     *
     * @param keyType the type of the keys stored in the map
     * @param valueType the type of the values stored in the map
     * @param path the path of the database file that will be used to store the map
     * @param tableName the name of the database table that will be used to store key-value pairs
     * @param temporary if {@code true}, the table will be dropped when calling {@link close}
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
        if (typedKey == null) {
            return false;
        }
        ensureConnectionNotClosed();
        try {
            return (countKeys(typedKey) > 0L);
        } catch(final Exception e) {
            throw new SQLiteMapException("Failed to determine if key exists!", e);
        }
    }

    @Override
    public boolean containsValue(final Object value) {
        final V typedValue = typeV.fromObject(value);
        if (typedValue == null) {
            return false;
        }
        ensureConnectionNotClosed();
        try {
            return (countValues(typedValue) > 0L);
        } catch(final Exception e) {
            throw new SQLiteMapException("Failed to determine if value exists!", e);
        }
    }

    @Override
    public V get(final Object key) {
        final K typedKey = typeK.fromObject(key);
        if (typedKey == null) {
            return null;
        }
        ensureConnectionNotClosed();
        try {
            return fetchEntry(typedKey);
        } catch (final Exception e) {
            throw new SQLiteMapException("Failed to read existing key-value pair!", e);
        }
    }

    @Override
    public V getOrDefault(final Object key, final V defaultValue) {
        final K typedKey = typeK.fromObject(key);
        if (typedKey == null) {
            return defaultValue;
        }
        ensureConnectionNotClosed();
        try {
            final V value = fetchEntry(typedKey);
            return (value != null) ? value : defaultValue;
        } catch (final Exception e) {
            throw new SQLiteMapException("Failed to read existing key-value pair!", e);
        }
    }

    @Override
    public V put(final K key, final V value) {
        Objects.requireNonNull(key, "Key must not be null!");
        Objects.requireNonNull(value, "Value must not be null!");
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
     *
     * @param key
     * @param value
     */
    public void put0(final K key, final V value) {
        Objects.requireNonNull(key, "Key must not be null!");
        Objects.requireNonNull(value, "Value must not be null!");
        ensureConnectionNotClosed();
        try {
            upsertEntry(key, value);
        } catch (final Exception e) {
            throw new SQLiteMapException("Failed to store new key-value pair!", e);
        }
    }

    @Override
    public V putIfAbsent(final K key, final V value) {
        Objects.requireNonNull(key, "Key must not be null!");
        Objects.requireNonNull(value, "Value must not be null!");
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
     *
     * @param key
     * @param value
     */
    public void putIfAbsent0(final K key, final V value) {
        Objects.requireNonNull(key, "Key must not be null!");
        Objects.requireNonNull(value, "Value must not be null!");
        ensureConnectionNotClosed();
        try {
            insertEntryOptional(key, value);
        } catch (final Exception e) {
            throw new SQLiteMapException("Failed to store new key-value pair!", e);
        }
    }

    @Override
    public void putAll(final Map<? extends K, ? extends V> map) {
        Objects.requireNonNull(map, "Map must not be null!");
        ensureConnectionNotClosed();
        try {
            transaction(connection, () -> upsertAllEntries(map.entrySet()));
        } catch (final Exception e) {
            throw new SQLiteMapException("Failed to store new key-value pairs!", e);
        }
    }

    /**
     * Add <i>multiple</i> key-value pairs to the map, using a <i>single</i> "batch" insert operation (performance optimization)
     *
     * @param entries a collection containing the key-value pairs to be added to the map
     */
    public void putAll(final Collection<? extends Entry<? extends K, ? extends V>> entries) {
        Objects.requireNonNull(entries, "Collection must not be null!");
        ensureConnectionNotClosed();
        try {
            transaction(connection, () -> upsertAllEntries(entries));
        } catch (final Exception e) {
            throw new SQLiteMapException("Failed to store new key-value pairs!", e);
        }
    }

    @Override
    public V compute(final K key, final BiFunction<? super K,? super V,? extends V> remappingFunction) {
        Objects.requireNonNull(key, "Key must not be null!");
        Objects.requireNonNull(remappingFunction, "Re-mapping function must not be null!");
        ensureConnectionNotClosed();
        try {
            return transaction(connection, () -> {
                final V previousValue = fetchEntry(key);
                final V value = remappingFunction.apply(key, previousValue);
                if (value == null) {
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
        Objects.requireNonNull(key, "Key must not be null!");
        Objects.requireNonNull(mappingFunction, "Mapping function must not be null!");
        ensureConnectionNotClosed();
        try {
            return transaction(connection, () -> {
                V value;
                if ((value = fetchEntry(key)) == null) {
                    if ((value = mappingFunction.apply(key)) != null) {
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
        Objects.requireNonNull(key, "Key must not be null!");
        Objects.requireNonNull(remappingFunction, "Re-mapping function must not be null!");
        ensureConnectionNotClosed();
        try {
            return transaction(connection, () -> {
                V value;
                if ((value = fetchEntry(key)) != null) {
                    if ((value = remappingFunction.apply(key, value)) != null) {
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
        Objects.requireNonNull(key, "Key must not be null!");
        Objects.requireNonNull(value, "Value must not be null!");
        ensureConnectionNotClosed();
        try {
            return transaction(connection, () -> {
                final V previousValue = fetchEntry(key);
                if (previousValue != null) {
                    updateEntry(key, value);
                }
                return previousValue;
            });
        } catch (final Exception e) {
            throw new SQLiteMapException("Failed to store new key-value pair!", e);
        }
    }

    @Override
    public boolean replace(final K key, final V oldValue, final V newValue) {
        Objects.requireNonNull(key, "Key must not be null!");
        Objects.requireNonNull(newValue, "Value must not be null!");
        ensureConnectionNotClosed();
        try {
            return transaction(connection, () -> {
                final V previousValue = fetchEntry(key);
                if ((previousValue != null) && typeV.equals(previousValue, oldValue)) {
                    updateEntry(key, newValue);
                    return true;
                }
                return false;
            });
        } catch (final Exception e) {
            throw new SQLiteMapException("Failed to store new key-value pair!", e);
        }
    }

    /**
     * A version of {@link replace} that does <b>not</b> return the previous value (performance optimization)
     */
    public void replace0(final K key, final V value) {
        Objects.requireNonNull(key, "Key must not be null!");
        Objects.requireNonNull(value, "Value must not be null!");
        ensureConnectionNotClosed();
        try {
            updateEntry(key, value);
        } catch (final Exception e) {
            throw new SQLiteMapException("Failed to store new key-value pair!", e);
        }
    }

    @Override
    public V remove(Object key) {
        final K typedKey = typeK.fromObject(key);
        if (typedKey == null) {
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
    public boolean remove(final Object key, final Object value) {
        final K typedKey = typeK.fromObject(key);
        if (typedKey == null) {
            return false;
        }
        ensureConnectionNotClosed();
        try {
            return transaction(connection, () -> {
                final V previousValue = fetchEntry(typedKey);
                if ((previousValue != null) && typeV.equals(previousValue, value)) {
                    removeEntry(typedKey);
                    return true;
                }
                return false;
            });
        } catch (final Exception e) {
            throw new SQLiteMapException("Failed to remove key-value pair!", e);
        }
    }

    /**
     * A version of {@link remove} that does <b>not</b> return the previous value (performance optimization)
     */
    public void remove0(Object key) {
        final K typedKey = typeK.fromObject(key);
        if (typedKey == null) {
            return;
        }
        ensureConnectionNotClosed();
        try {
            removeEntry(typedKey);
        } catch (final Exception e) {
            throw new SQLiteMapException("Failed to remove key-value pair!", e);
        }
    }

    @Override
    public void clear() {
        ensureConnectionNotClosed();
        final PreparedStatement preparedStatement = sqlClearEntries.getInstance();
        try {
            preparedStatement.executeUpdate();
            ++generation;
        } catch(final Exception e) {
            throw new SQLiteMapException("Failed to clear existing key-value pairs!", e);
        }
    }

    @Override
    public SQLiteMapKeySet keySet() {
        if (keySet == null) {
            keySet = new SQLiteMapKeySet();
        }
        return keySet;
    }

    @Override
    public SQLiteMapEntrySet entrySet() {
        if (entrySet == null) {
            entrySet = new SQLiteMapEntrySet();
        }
        return entrySet;
    }

    @Override
    public SQLiteMapValueCollection values() {
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

    @Override
    public boolean isEmpty() {
        ensureConnectionNotClosed();
        try {
            return (!(countEntries() > 0L));
        } catch(final Exception e) {
            throw new SQLiteMapException("Failed to count the number of key-value pairs!", e);
        }
    }

    /**
     * A version of {@link size} that returns the number key-value pairs in this map as a <b>long</b> value.
     * This method works correctly with maps containing more than {@code Integer.MAX_VALUE} elements.
     * @return the number of key-value pairs in this map
     */
    public long sizeLong() {
        ensureConnectionNotClosed();
        try {
            final long count = countEntries();
            if (count < 0) {
                throw new IllegalStateException("Entry count could not be determined!");
            }
            return count;
        } catch(final Exception e) {
            throw new SQLiteMapException("Failed to count the number of key-value pairs!", e);
        }
    }

    @Override
    public int size() {
        return Math.toIntExact(sizeLong());
    }

    /**
     * Returns an iterator over the key-value pairs in this map. The key-value pairs are returned in no particular order.
     * <p>
     * <b>Important notice:</b> The returned iterator <i>must</i> explicitly be {@link close}'d when it is no longer needed!
     *
     * @return an iterator over the key-value pairs in this map
     */
    @Override
    public SQLiteMapEntryIterator iterator() {
        ensureConnectionNotClosed();
        final PreparedStatement preparedStatement = sqlFetchEntries.getInstance();
        try {
            return new SQLiteMapEntryIterator(preparedStatement.executeQuery(), generation);
        } catch(final Exception e) {
            throw new SQLiteMapException("Failed to query the existing key-value pairs!", e);
        }
    }

    /**
     * Returns an iterator over the keys in this map. The keys are returned in no particular order.
     * <p>
     * <b>Important notice:</b> The returned iterator <i>must</i> explicitly be {@link close}'d when it is no longer needed!
     *
     * @return an iterator over the keys in this map
     */
    public SQLiteMapKeyIterator keyIterator() {
        ensureConnectionNotClosed();
        final PreparedStatement preparedStatement = sqlFetchKeys.getInstance();
        try {
            return new SQLiteMapKeyIterator(preparedStatement.executeQuery(), generation);
        } catch(final Exception e) {
            throw new SQLiteMapException("Failed to query the existing key set!", e);
        }
    }

    /**
     * Returns an iterator over the values in this map. The values are returned in no particular order.
     * <p>
     * <b>Important notice:</b> The returned iterator <i>must</i> explicitly be {@link close}'d when it is no longer needed!
     *
     * @return an iterator over the values in this map
     */
    public SQLiteMapValueIterator valueIterator() {
        ensureConnectionNotClosed();
        final PreparedStatement preparedStatement = sqlFetchValues.getInstance();
        try {
            return new SQLiteMapValueIterator(preparedStatement.executeQuery(), generation);
        } catch(final Exception e) {
            throw new SQLiteMapException("Failed to query the existing value set!", e);
        }
    }

    /**
     * Create an index on the "value" column or drop the existing index.
     * <p>
     * An index on the "value" column can significantly speed up <b>value</b> <i>lookup</i> operations, but it comes at a
     * certain memory overhead and may slow down <i>insert</i> operations. Initially, there is <b>no</b> "value" index.
     *
     * @param enable if {@code true} creates the index, otherwise drops the index
     */
    public void setValueIndexEnabled(final boolean enable) {
        ensureConnectionNotClosed();
        final PreparedStatement preparedStatement = enable ? sqlCreateIndex.getInstance() : sqlDestroyIndex.getInstance();
        try {
            preparedStatement.executeUpdate();
        } catch(final Exception e) {
            throw new SQLiteMapException("Failed to create or drop the value index!", e);
        }
    }

    @Override
    public boolean equals(final Object o) {
        if (o == this)
            return true;
        else if (o instanceof Map) {
            final Map<?, ?> map = (Map<?, ?>) o;
            try (final SQLiteMapEntryIterator iter = iterator()) {
                while(iter.hasNext()) {
                    final Entry<K, V> current = iter.next();
                    final Object value = map.get(current.getKey());
                    if ((value == null) || (!typeV.equals(current.getValue(), value))) {
                        return false;
                    }
                }
                return true;
            }
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        if (hashCodeGen != generation) {
            try (final SQLiteMapEntryIterator iter = iterator()) {
                int hashCode = 0;
                while(iter.hasNext()) {
                    hashCode += iter.next().hashCode();
                }
                this.hashCode = hashCode;
                hashCodeGen = generation;
            }
        }
        return hashCode;
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
                        destroySQLiteTable();
                    }
                } finally {
                    closePendigInstances();
                    connection.close();
                }
            } catch (final Exception e) {
                throw new SQLiteMapException("Failed to close SQLite connection!", e);
            }
        }
    }

    // --------------------------------------------------------
    // Unsupported Operations
    // --------------------------------------------------------

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

    private void createSQLiteTable() {
        try (final PreparedStatement preparedStatement = connection.prepareStatement(String.format(SQL_CREATE_TABLE, tableName, typeK.typeName(), typeV.typeName()))) {
            preparedStatement.executeUpdate();
        } catch (final Exception e) {
            throw new SQLiteMapException("Failed to create SQLitabe table!", e);
        }
        try (final ResultSet result = connection.getMetaData().getColumns(null, null, tableName, null)) {
            int columnCount = 0;
            while(result.next()) {
                switch(++columnCount) {
                case 1:
                    verifyProperty(result, "COLUMN_NAME", "key");
                    verifyProperty(result, "DATA_TYPE", String.valueOf(typeK.getTypeId()));
                    break;
                case 2:
                    verifyProperty(result, "COLUMN_NAME", "value");
                    verifyProperty(result, "DATA_TYPE", String.valueOf(typeV.getTypeId()));
                    break;
                }
            }
            if (columnCount != 2) {
                throw new IllegalStateException("Invalid number of columns detected! [columnCount=" + columnCount + "]");
            }
        } catch (final Exception e) {
            throw new SQLiteMapException("Failed to verify SQLitabe table!", e);
        }
    }

    private void destroySQLiteTable() {
        try (final PreparedStatement preparedStatement = connection.prepareStatement(String.format(SQL_DESTROY_TABLE, tableName))) {
            preparedStatement.executeUpdate();
            ++generation;
        } catch (final Exception e) {
            throw new SQLiteMapException("Failed to destroy SQLitabe table!", e);
        }
    }

    private long countEntries() {
        final PreparedStatement preparedStatement = sqlCountEntries.getInstance();
        try {
            try (final ResultSet result = preparedStatement.executeQuery()) {
                if (result.next()) {
                    return result.getLong(1);
                }
                return -1L;
            }
        } catch(final SQLException e) {
            throw new SQLiteMapException("Failed to count existing entries!", e);
        }
    }

    private long countKeys(final K key) {
        final PreparedStatement preparedStatement = sqlCountKeys.getInstance();
        try {
            typeK.setParameter(preparedStatement, 1, key);
            try (final ResultSet result = preparedStatement.executeQuery()) {
                if (result.next()) {
                    return result.getLong(1);
                }
                return -1L;
            } finally {
                preparedStatement.clearParameters();
            }
        } catch(final SQLException e) {
            throw new SQLiteMapException("Failed to count existing keys!", e);
        }
    }

    private long countValues(final V value) {
        final PreparedStatement preparedStatement = sqlCountValues.getInstance();
        try {
            typeV.setParameter(preparedStatement, 1, value);
            try (final ResultSet result = preparedStatement.executeQuery()) {
                if (result.next()) {
                    return result.getLong(1);
                }
                return -1L;
            } finally {
                preparedStatement.clearParameters();
            }
        } catch(final SQLException e) {
            throw new SQLiteMapException("Failed to count existing values!", e);
        }
    }

    private V fetchEntry(final K key) {
        final PreparedStatement preparedStatement = sqlFetchEntry.getInstance();
        try  {
            typeK.setParameter(preparedStatement, 1, key);
            try (final ResultSet result = preparedStatement.executeQuery()) {
                if (result.next()) {
                    return typeV.getResult(result, 1);
                }
            } finally {
                preparedStatement.clearParameters();
            }
        } catch(final SQLException e) {
            throw new SQLiteMapException("Failed to query the existing value!", e);
        }
        return null;
    }

    private void insertEntry(final K key, final V value) {
        final PreparedStatement preparedStatement = sqlInsertEntry.getInstance();
        try {
            typeK.setParameter(preparedStatement, 1, key);
            typeV.setParameter(preparedStatement, 2, value);
            try {
                preparedStatement.executeUpdate();
                ++generation;
            } finally {
                preparedStatement.clearParameters();
            }
        } catch (final SQLException e) {
            throw new SQLiteMapException("Failed to insert the new key-value pair!", e);
        }
    }

    private void insertEntryOptional(final K key, final V value) {
        final PreparedStatement preparedStatement = sqlInsertEntry0.getInstance();
        try {
            typeK.setParameter(preparedStatement, 1, key);
            typeV.setParameter(preparedStatement, 2, value);
            try {
                preparedStatement.executeUpdate();
                ++generation;
            } finally {
                preparedStatement.clearParameters();
            }
        } catch (final SQLException e) {
            throw new SQLiteMapException("Failed to insert the new key-value pair!", e);
        }
    }

    private void upsertEntry(final K key, final V value) {
        final PreparedStatement preparedStatement = sqlUpsertEntry.getInstance();
        try {
            typeK.setParameter(preparedStatement, 1, key);
            typeV.setParameter(preparedStatement, 2, value);
            try {
                preparedStatement.executeUpdate();
                ++generation;
            } finally {
                preparedStatement.clearParameters();
            }
        } catch (final SQLException e) {
            throw new SQLiteMapException("Failed to upsert the new key-value pair!", e);
        }
    }

    private void upsertAllEntries(final Collection<? extends Entry<? extends K, ? extends V>> entries) {
        final PreparedStatement preparedStatement = sqlUpsertEntry.getInstance();
        try {
            try {
                int currentBatchSize = 0;
                for(final Entry<? extends K, ? extends V> entry : entries) {
                    typeK.setParameter(preparedStatement, 1, Objects.requireNonNull(entry.getKey(), "Key must not be null!"));
                    typeV.setParameter(preparedStatement, 2, Objects.requireNonNull(entry.getValue(), "Value must not be null!"));
                    preparedStatement.addBatch();
                    if (++currentBatchSize >= MAXIMUM_BATCH_SIZE) {
                        preparedStatement.executeBatch();
                        currentBatchSize = 0;
                    }
                }
                if (currentBatchSize > 0) {
                    preparedStatement.executeBatch();
                }
                ++generation;
            } finally {
                preparedStatement.clearBatch();
                preparedStatement.clearParameters();
            }
        } catch (final SQLException e) {
            throw new SQLiteMapException("Failed to upsert the new key-value pairs!", e);
        }
    }

    private void updateEntry(final K key, final V value) {
        final PreparedStatement preparedStatement = sqlUpdateEntry.getInstance();
        try {
            typeK.setParameter(preparedStatement, 2, key);
            typeV.setParameter(preparedStatement, 1, value);
            try {
                preparedStatement.executeUpdate();
                ++generation;
            } finally {
                preparedStatement.clearParameters();
            }
        } catch (final SQLException e) {
            throw new SQLiteMapException("Failed to insert the new key-value pair!", e);
        }
    }

    private void removeEntry(final K key) {
        final PreparedStatement preparedStatement = sqlRemoveEntry.getInstance();
        try {
            typeK.setParameter(preparedStatement, 1, key);
            try {
                preparedStatement.executeUpdate();
                ++generation;
           } finally {
                preparedStatement.clearParameters();
            }
        } catch (final SQLException e) {
            throw new SQLiteMapException("Failed to remove the key-value pair!", e);
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> Type<T> typeOf(final Class<T> clazz) {
        Objects.requireNonNull(clazz, "Class parameter must not be null!");
        final Type<?> type = TYPE_MAP.get(clazz);
        if (type == null) {
            throw new IllegalArgumentException("Unsupported type: " + clazz.getName());
        }
        return (Type<T>) type;
    }

    private static void verifyProperty(final ResultSet result, final String property, final String expected) throws SQLException {
        final String value = result.getString(property);
        if (!expected.equals(value)) {
            throw new IllegalStateException("Mismatch in property \"" + property + "\" detected! [expected \"" + expected + "\", but was \"" + value + "\"]");
        }
    }

    private void closePendigInstances() {
        AutoCloseable obj;
        while((obj = cleanUpQueue.pollLast()) != null) {
            try {
                obj.close();
            } catch (Exception e) { }
        }
    }

    private static void transaction(final Connection connection, final Runnable runnable) throws SQLException {
        transaction(connection, () -> {
            runnable.run();
            return (Void) null;
        });
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
