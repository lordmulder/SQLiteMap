/*
 * SQLiteMap was created by LoRd_MuldeR <mulder2@gmx.de>.
 *
 * To the extent possible under law, the person who associated CC0 with SQLiteMap has waived all copyright and related or
 * neighboring rights to SQLiteMap. You should have received a copy of the CC0 legalcode along with this work.
 *
 * If not, please refer to:
 * http://creativecommons.org/publicdomain/zero/1.0/
 */
package com.muldersoft.container.sqlite;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.Spliterator;
import java.util.concurrent.Callable;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * The <b>{@code SQLiteMap}</b> class provides a {@link Map} implementation that is backed by an <i>SQLite</i> database. It can
 * employ an "in-memory" database as well as a local database file. Compared to Java's standard {@code HashMap} class,
 * the "in-memory" variant of {@code SQLiteMap} is better suited for <i>very large</i> maps; it has a smaller memory footprint
 * and it does <b>not</b> clutter the Java heap space. The file-based variant of {@code SQLiteMap} provides full persistence.
 * <p>
 * {@code SQLiteMap} requires the <a href="https://mvnrepository.com/artifact/org.xerial/sqlite-jdbc">SQLite JDBC Driver</a>,
 * version 3.36 or newer, to be available in the classpath at runtime!
 * <p>
 * New instances of {@code SQLiteMap} that are backed by an "in-memory" database or by a file-based database can be created by
 * calling the static method {@link #fromMemory fromMemory()} or {@link #fromFile fromFile()}, respectively. Because
 * {@code SQLiteMap} is backed by an SQLite database, the <i>types</i> supported as keys and values are restricted. For the
 * time being, <i>only</i> the types {@code String}, {@code Boolean}, {@code Byte}, {@code byte[]}, {@code Integer},
 * {@code Long}, {@code Instant} as well as {@code BigInteger} are supported. Other types may be stored via serialization.
 * <p>
 * This class is <b>not</b> "thread-safe", in the sense that the <i>same</i> instance of {@code SQLiteMap} <b>must not</b> be
 * accessed concurrently by <i>different</i> threads. However, it is perfectly "safe" to created <i>multiple</i> instances of
 * {@code SQLiteMap} in <i>different</i> threads; each instance uses its own separate SQLite connection and its own separate
 * database table. As long as each thread <i>only</i> accesses its own instance an does <i>not</i> share that instance with
 * other threads, <b>no</b> synchronization is required. In case that the <i>same</i> instance of {@code SQLiteMap} needs to be
 * shared across <i>different</i> threads, the application <b>must</b> explicitly <i>synchronize</i> <b>all</b> accesses to
 * that "shared" instance! This includes any iterators, key/entry sets or value collections returned by this class.
 * <p>
 * The methods {@link iterator}, {@link keyIterator} and {@link valueIterator} as well as the corresponding methods of the view
 * objects provided by {@link #entrySet()}, {@link #keySet()} and {@link values} are <b>not</b> "reentrant", in the sense that
 * the iterators created by these methods <b>must</b> explicitly be {@code close()}'d <i>before</i> another iterator  may be
 * created. The methods {@link hashCode}, {@link #equals equals()} and {@link #forEach forEach()} <b>must not</b> be called
 * while an iteration is in progress. Creating a new iterator is <b>not</b> allowed from a {@link #forEach forEach()} action.
 * <p>
 * {@code SQLiteMap}, <i>in general</i>, makes <b>no</b> guarantees as to the order of the map; in particular, it does
 * <b>not</b> guarantee that the order will remain constant over time. However, overloaded iterator methods are provided which
 * allow for enforcing an <i>explicit</i> {@link SQLiteMap.IterationOrder iteration order}. Additionally, the <i>default</i>
 * iteration order can be stipulate via the corresponding setter methods. Enforcing an <i>explicit</i> iteration order may
 * degrade the iterator's performance, compared to {@code UNSPECIFIED} order.
 * <p>
 * All iterators returned by this class's iterator methods are <i>"fail-fast"</i>: if the map is modified at any time after the
 * iterator was created, then the iterator throws a {@link ConcurrentModificationException} when the next element is accessed.
 * This <i>only</i> applies to modifications induced by the same {@code SQLiteMap} instance. In case that the underlying
 * database table is modified <i>"externally"</i>, these modifications will <b>not</b> be reflected by the existing iterator!
 * <p>
 * The {@link #merge merge()}, {@link #replaceAll replaceAll()} and {@link spliterator} methods currently are <b>not</b>
 * supported by this class.
 * <p>
 * This {@code Map} implementation does <b>not</b> support {@code null} keys or {@code null} values.
 * <p>
 * <b>Important notice:</b> Instances of this class <i>must</i> explicitly be {@link close}'d when no longer needed. If an
 * {@code SQLiteMap} instance is <b>not</b> properly closed, a <i>resource leak</i> occurs, because the underlying database
 * connection is <i>never</i> closed. Also, if using a "temporary" database table, then that table is <i>not</i> dropped until
 * the {@code SQLiteMap} instance is closed. This class does <b>not</b> use finalizers to perform the required clean-up,
 * because finalizers are inherently unreliable and may even hurt performance.
 *
 * @param <K> the type of keys maintained by this map
 * @param <V> the type of mapped values
 *
 * @author Created by LoRd_MuldeR &lt;mulder2@gmx.de&gt;
 * @see <a href="https://github.com/lordmulder/SQLiteMap">SQLiteMap (GitHub project)</a>
 * @see <a href="https://www.sqlite.org/index.html">SQLite Home Page</a>
 * @see <a href="https://github.com/xerial/sqlite-jdbc">SQLite JDBC Driver (GitHub project)</a>
 * @see <a href="https://muldersoft.com/">MuldeR's OpenSource Projects</a>
 */
public final class SQLiteMap<K,V> implements Map<K,V>, Iterable<Map.Entry<K, V>>, AutoCloseable {

    private static final short VERSION_MAJOR = 1;
    private static final short VERSION_MINOR = 1;
    private static final short VERSION_PATCH = 0;

    private static final String SQLITE_JDBC_DRIVER = "org.sqlite.JDBC";
    private static final String SQLITE_JDBC_PREFIX = "jdbc:sqlite:";
    private static final String SQLITE_INMEMORY_DB = ":memory:";
    private static final String SQLITE_NAME_PREFIX = String.format("SQLiteMap-%d:", VERSION_MAJOR);

    private static final Pattern REGEX_TABLE_NAME = Pattern.compile("\\w+");

    private static final int MAXIMUM_BATCH_SIZE = 256;

    private static final String EMPTY_STRING = new String();

    // ======================================================================
    // Instance Variables
    // ======================================================================

    private Connection connection;

    private final Type<K> typeK;
    private final Type<V> typeV;

    private final String dbTableName;
    private final Path dbFilePath;
    private final boolean deleteFile;
    private final boolean dropTable;

    private SQLiteMapEntrySet entrySet = null;
    private SQLiteMapKeySet keySet = null;
    private SQLiteMapValueCollection valueCollection = null;

    private long modifyCount;

    private final Set<Statement> pendingIterators = new LinkedHashSet<Statement>();
    private final Set<AutoCloseable> cleanUpQueue = new LinkedHashSet<AutoCloseable>();

    private IterationOrder defaultKeyOrder   = IterationOrder.UNSPECIFIED;
    private IterationOrder defaultValueOrder = IterationOrder.UNSPECIFIED;

    // ======================================================================
    // SQL Statements
    // ======================================================================

    private static final String SQL_JOURNAL_MODE  = "PRAGMA journal_mode=%s;";
    private static final String SQL_CREATE_TABLE  = "CREATE TABLE IF NOT EXISTS `%s` (key %s CONSTRAINT pk_key PRIMARY KEY, value %s NOT NULL);";
    private static final String SQL_DESTROY_TABLE = "DROP TABLE `%s`;";
    private static final String SQL_CREATE_INDEX  = "CREATE INDEX IF NOT EXISTS `%1$s~index` ON `%1$s` (value);";
    private static final String SQL_DESTROY_INDEX = "DROP INDEX IF EXISTS `%s~index`;";
    private static final String SQL_FETCH_ENTRY   = "SELECT value FROM `%s` WHERE key = ?;";
    private static final String SQL_FETCH_KEYS    = "SELECT key FROM `%s`;";
    private static final String SQL_FETCH_VALUES  = "SELECT value FROM `%s`;";
    private static final String SQL_FETCH_ENTRIES = "SELECT key, value FROM `%s`;";
    private static final String SQL_SORT_ENTRIES0 = "SELECT key, value FROM `%s` ORDER BY key ASC;";
    private static final String SQL_SORT_ENTRIES1 = "SELECT key, value FROM `%s` ORDER BY key DESC;";
    private static final String SQL_SORT_KEYS0    = "SELECT key FROM `%s` ORDER BY key ASC;";
    private static final String SQL_SORT_KEYS1    = "SELECT key FROM `%s` ORDER BY key DESC;";
    private static final String SQL_SORT_VALUES0  = "SELECT value FROM `%s` ORDER BY value ASC;";
    private static final String SQL_SORT_VALUES1  = "SELECT value FROM `%s` ORDER BY value DESC;";
    private static final String SQL_INSERT_ENTRY0 = "INSERT INTO `%s` (key, value) VALUES (?, ?);";
    private static final String SQL_INSERT_ENTRY1 = "INSERT INTO `%s` (key, value) VALUES (?, ?) ON CONFLICT(key) DO NOTHING;";
    private static final String SQL_UPSERT_ENTRY  = "INSERT INTO `%s` (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value;";
    private static final String SQL_UPDATE_ENTRY0 = "UPDATE `%s` SET value = ? WHERE key = ?;";
    private static final String SQL_UPDATE_ENTRY1 = "UPDATE `%s` SET value = ? WHERE key = ? AND value = ?;";
    private static final String SQL_REMOVE_ENTRY0 = "DELETE FROM `%s` WHERE key = ?;";
    private static final String SQL_REMOVE_ENTRY1 = "DELETE FROM `%s` WHERE key = ? AND value = ?;";
    private static final String SQL_CLEAR_ENTRIES = "DELETE FROM `%s`;";
    private static final String SQL_COUNT_ENTRIES = "SELECT COUNT(*) FROM `%s`;";
    private static final String SQL_COUNT_KEYS    = "SELECT COUNT(*) FROM `%s` WHERE key = ?;";
    private static final String SQL_COUNT_VALUES  = "SELECT COUNT(*) FROM `%s` WHERE value = ? LIMIT 1;";

    private final PreparedStatementHolder sqlFetchEntry   = new PreparedStatementHolder(SQL_FETCH_ENTRY);
    private final PreparedStatementHolder sqlFetchKeys    = new PreparedStatementHolder(SQL_FETCH_KEYS);
    private final PreparedStatementHolder sqlFetchValues  = new PreparedStatementHolder(SQL_FETCH_VALUES);
    private final PreparedStatementHolder sqlFetchEntries = new PreparedStatementHolder(SQL_FETCH_ENTRIES);
    private final PreparedStatementHolder sqlSortEntries0 = new PreparedStatementHolder(SQL_SORT_ENTRIES0);
    private final PreparedStatementHolder sqlSortEntries1 = new PreparedStatementHolder(SQL_SORT_ENTRIES1);
    private final PreparedStatementHolder sqlSortKeys0    = new PreparedStatementHolder(SQL_SORT_KEYS0);
    private final PreparedStatementHolder sqlSortKeys1    = new PreparedStatementHolder(SQL_SORT_KEYS1);
    private final PreparedStatementHolder sqlSortValues0  = new PreparedStatementHolder(SQL_SORT_VALUES0);
    private final PreparedStatementHolder sqlSortValues1  = new PreparedStatementHolder(SQL_SORT_VALUES1);
    private final PreparedStatementHolder sqlInsertEntry0 = new PreparedStatementHolder(SQL_INSERT_ENTRY0);
    private final PreparedStatementHolder sqlInsertEntry1 = new PreparedStatementHolder(SQL_INSERT_ENTRY1);
    private final PreparedStatementHolder sqlUpsertEntry  = new PreparedStatementHolder(SQL_UPSERT_ENTRY);
    private final PreparedStatementHolder sqlUpdateEntry0 = new PreparedStatementHolder(SQL_UPDATE_ENTRY0);
    private final PreparedStatementHolder sqlUpdateEntry1 = new PreparedStatementHolder(SQL_UPDATE_ENTRY1);
    private final PreparedStatementHolder sqlRemoveEntry0 = new PreparedStatementHolder(SQL_REMOVE_ENTRY0);
    private final PreparedStatementHolder sqlRemoveEntry1 = new PreparedStatementHolder(SQL_REMOVE_ENTRY1);
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
                    statement = connection.prepareStatement(String.format(sql, dbTableName));
                } catch (final Exception e) {
                    throw new SQLiteMapException("Failed to prepare SQL statement!", e);
                }
                cleanUpQueue.add(this);
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
     * Exception class to indicate {@link SQLiteMap} errors.
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
    // Order
    // ======================================================================

    /**
     * Specifies the iteration order, for iterator methods that support ordering.
     */
    public enum IterationOrder {
        /**
         * The elements are returned in <b>no</b> particular order. This generally is the <i>fastest</i> iteration order,
         * because SQLite can return the elements in whatever order is expected to give the best performance.
         * <p>
         * This can appear to be equivalent to {@code ASCENDING} order, but do <b>not</b> rely on that!
         */
        UNSPECIFIED,
        /**
         * Forces the elements to be returned in <i>ascending</i> "natural" order. May be slower than {@code UNSPECIFIED}.
         */
        ASCENDING,
        /**
         * Forces the elements to be returned in <i>descending</i> "natural" order. May be slower than {@code UNSPECIFIED}.
         */
        DESCENDING
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

    /**
     * Represents a specific mapping (key-value pair) in the {@link SQLiteMap}.
     */
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
            return typeK.hashCode(key) ^ typeV.hashCode(value);
        }

        @Override
        public boolean equals(final Object o) {
            if (o == this) {
                return true;
            }
            if (!(o instanceof Entry)) {
                return false;
            }
            final Entry<?, ?> entry = (Entry<?, ?>) o;
            return typeK.equals(key, entry.getKey()) && typeV.equals(value, entry.getValue());
        }
    }

    // ======================================================================
    // Map Iterator
    // ======================================================================

    /**
     * Base class for {@link SQLiteMap} iterators.
     */
    protected abstract class SQLiteMapAbstractIterator<T> implements Iterator<T>, AutoCloseable {
        protected final long modifyCount;
        protected final PreparedStatement statement;
        protected ResultSet resultSet;
        protected boolean pending = false;

        protected SQLiteMapAbstractIterator(final PreparedStatement statement) throws Exception {
            modifyCount = SQLiteMap.this.modifyCount;
            if (!pendingIterators.add(this.statement = Objects.requireNonNull(statement))) {
                throw new IllegalStateException("Cannot create new iterator while previous iterator is still active!");
            }
            try {
                resultSet = Objects.requireNonNull(statement.executeQuery(), "Result set must not be null!");
            } catch (final Exception e) {
                pendingIterators.remove(statement);
                throw e;
            }
            cleanUpQueue.add(this);
        }

        @Override
        public boolean hasNext() {
            try {
                ensureIteratorNotClosed();
                if (!pending) {
                    pending = fetchNextElement();
                }
                return pending;
            } catch (IllegalStateException e) {
                return false;
            }
        }

        protected boolean fetchNextElement() {
            try {
                if (!resultSet.next()) {
                    close();
                    return false;
                }
                return true;
            } catch (final SQLException e) {
                throw new SQLiteMapException("Failed to query next element!", e);
            }
        }

        protected void ensureIteratorNotClosed() {
            SQLiteMap.this.ensureConnectionNotClosed();
            if (resultSet == null) {
                throw new IllegalStateException("Iterator has already been closed!");
            }
        }

        protected void ensureMapHasNotBeenModified() throws SQLException {
            if (modifyCount != SQLiteMap.this.modifyCount) {
                try {
                    close();
                } catch (Exception e) { }
                throw new ConcurrentModificationException("The map has been modified after this iterator was created!");
            }
        }

        @Override
        public void close() {
            if (resultSet != null) {
                try {
                    resultSet.close();
                } catch (final SQLException e) {
                    throw new SQLiteMapException("Failed to clean up iterator!", e);
                } finally {
                    resultSet = null;
                    pendingIterators.remove(statement);
                    cleanUpQueue.remove(this);
                }
            }
        }
    }

    /**
     * Iterator implementation for iterating {@link SQLiteMap} keys.
     * <p>
     * <b>Important notice:</b> Instances of this class <i>must</i> explicitly be {@link #close close()}'d when they are no
     * longer needed!
     * <p>
     * The iterator is closed <i>implicitly</i> when the end of the iteration was reached, i.e. when {@link #hasNext hasNext()}
     * has returned {@code false} or when {@link #next next()} has thrown a {@code NoSuchElementException} exception.
     */
    public class SQLiteMapKeyIterator extends SQLiteMapAbstractIterator<K> {
        private SQLiteMapKeyIterator(final PreparedStatement statement) throws Exception {
            super(statement);
        }

        @Override
        public K next() {
            ensureIteratorNotClosed();
            if ((!pending) && (!fetchNextElement())) {
                throw new NoSuchElementException("No more elements are available!");
            }
            try {
                ensureMapHasNotBeenModified();
                return typeK.getResult(resultSet, 1);
            } catch (final SQLException e) {
                throw new SQLiteMapException("Failed to read next key-value pair!", e);
            } finally {
                pending = false; /*reset state!*/
            }
        }
    }

    /**
     * Iterator implementation for iterating {@link SQLiteMap} values.
     * <p>
     * <b>Important notice:</b> Instances of this class <i>must</i> explicitly be {@link #close close()}'d when they are no
     * longer needed!
     * <p>
     * The iterator is closed <i>implicitly</i> when the end of the iteration was reached, i.e. when {@link #hasNext hasNext()}
     * has returned {@code false} or when {@link #next next()} has thrown a {@code NoSuchElementException} exception.
     */
    public class SQLiteMapValueIterator extends SQLiteMapAbstractIterator<V> {
        private SQLiteMapValueIterator(final PreparedStatement statement) throws Exception {
            super(statement);
        }

        @Override
        public V next() {
            ensureIteratorNotClosed();
            if ((!pending) && (!fetchNextElement())) {
                throw new NoSuchElementException("No more elements are available!");
            }
            try {
                ensureMapHasNotBeenModified();
                return typeV.getResult(resultSet, 1);
            } catch (final SQLException e) {
                throw new SQLiteMapException("Failed to read next key-value pair!", e);
            } finally {
                pending = false; /*reset state!*/
            }
        }
    }

    /**
     * Iterator implementation for iterating {@link SQLiteMap} entries.
     * <p>
     * <b>Important notice:</b> Instances of this class <i>must</i> explicitly be {@link #close close()}'d when they are no
     * longer needed!
     * <p>
     * The iterator is closed <i>implicitly</i> when the end of the iteration was reached, i.e. when {@link #hasNext hasNext()}
     * has returned {@code false} or when {@link #next next()} has thrown a {@code NoSuchElementException} exception.
     */
    public class SQLiteMapEntryIterator extends SQLiteMapAbstractIterator<Entry<K, V>> {
        private SQLiteMapEntryIterator(final PreparedStatement statement) throws Exception {
            super(statement);
        }

        @Override
        public Entry<K, V> next() {
            ensureIteratorNotClosed();
            if ((!pending) && (!fetchNextElement())) {
                throw new NoSuchElementException("No more elements are available!");
            }
            try {
                ensureMapHasNotBeenModified();
                return new SQLiteMapEntry(typeK.getResult(resultSet, 1), typeV.getResult(resultSet, 2));
            } catch (final SQLException e) {
                throw new SQLiteMapException("Failed to read next key-value pair!", e);
            } finally {
                pending = false; /*reset state!*/
            }
        }
    }

    // ======================================================================
    // Entry Set
    // ======================================================================

    /**
     * Base class for {@link SQLiteMap} set views.
     */
    protected abstract class SQLiteMapAbstractSet<T> implements Set<T> {
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
        public void clear() {
            SQLiteMap.this.clear();
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
        public Spliterator<T> spliterator() {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Set view implementation of the keys contained in an {@link SQLiteMap}.
     * <p>
     * The set is backed by the underlying map, so changes to the map are reflected in the set, and vice-versa.
     */
    public class SQLiteMapKeySet extends SQLiteMapAbstractSet<K> {
        @Override
        public boolean contains(final Object o) {
            return SQLiteMap.this.containsKey(o);
        }

        @Override
        public boolean containsAll(final Collection<?> c) {
            return SQLiteMap.this.containsAllKeys(c);
        }

        /**
         * Returns an iterator over the elements in this set. The elements are returned in no particular order.
         * <p>
         * <b>Important notice:</b> The returned iterator <i>must</i> explicitly be {@link close}'d when it is no longer needed!
         *
         * @return an iterator over elements in this set
         */
        @Override
        public SQLiteMapKeyIterator iterator() {
            return SQLiteMap.this.keyIterator();
        }

        /**
         * Returns an iterator over the elements in this set. The elements are returned in the specified order.
         * <p>
         * <b>Important notice:</b> The returned iterator <i>must</i> explicitly be {@link close}'d when it is no longer needed!
         *
         * @param order The order in which the elements of this set will be returned (ascending or descending)
         * @return an iterator over the elements in this set
         */
        public SQLiteMapKeyIterator iterator(final IterationOrder order) {
            return SQLiteMap.this.keyIterator(order);
        }

        @Override
        public boolean remove(final Object o) {
            return (SQLiteMap.this.remove(o) != null);
        }

        /**
         * A version of {@link remove} that does <b>not</b> return a result.
         * <p>
         * This method is a performance optimization and should be preferred whenever the result is <b>not</b> needed.
         *
         * @param o key whose mapping is to be removed from the map
         */
        public void remove0(final Object o) {
            SQLiteMap.this.remove0(o);
        }

        @Override
        public boolean removeAll(final Collection<?> c) {
            return SQLiteMap.this.removeAll(c);
        }

        @Override
        public void clear() {
            SQLiteMap.this.clear();
        }
    }

    /**
     * Set view implementation of the values contained in an {@link SQLiteMap}.
     * <p>
     * The set is backed by the underlying map, so changes to the map are reflected in the set, and vice-versa.
     */
    public class SQLiteMapValueCollection extends SQLiteMapAbstractSet<V> {
        @Override
        public boolean contains(final Object o) {
            return SQLiteMap.this.containsValue(o);
        }

        @Override
        public boolean containsAll(final Collection<?> c) {
            return SQLiteMap.this.containsAllValues(c);
        }

        /**
         * Returns an iterator over the elements in this set. The elements are returned in no particular order.
         * <p>
         * <b>Important notice:</b> The returned iterator <i>must</i> explicitly be {@link close}'d when it is no longer needed!
         *
         * @return an iterator over elements in this set
         */
        @Override
        public SQLiteMapValueIterator iterator() {
            return SQLiteMap.this.valueIterator();
        }

        /**
         * Returns an iterator over the elements in this set. The elements are returned in the specified order.
         * <p>
         * <b>Important notice:</b> The returned iterator <i>must</i> explicitly be {@link close}'d when it is no longer needed!
         *
         * @param order The order in which the elements of this set will be returned (ascending or descending)
         * @return an iterator over the elements in this set
         */
        public SQLiteMapValueIterator iterator(final IterationOrder order) {
            return SQLiteMap.this.valueIterator(order);
        }
    }

    /**
     * Set view implementation of the mappings contained in an {@link SQLiteMap}.
     * <p>
     * The set is backed by the underlying map, so changes to the map are reflected in the set, and vice-versa.
     */
    public class SQLiteMapEntrySet extends SQLiteMapAbstractSet<Entry<K, V>> {
        @Override
        public boolean contains(final Object o) {
            return SQLiteMap.this.containsEntry(o);
        }

        @Override
        public boolean containsAll(final Collection<?> c) {
            return SQLiteMap.this.containsAllEntries(c);
        }

        /**
         * Returns an iterator over the elements in this set. The elements are returned in no particular order.
         * <p>
         * <b>Important notice:</b> The returned iterator <i>must</i> explicitly be {@link close}'d when it is no longer needed!
         *
         * @return an iterator over elements in this set
         */
        @Override
        public SQLiteMapEntryIterator iterator() {
            return SQLiteMap.this.iterator();
        }

        /**
         * Returns an iterator over the elements in this set. The elements are returned in the specified order.
         * <p>
         * <b>Important notice:</b> The returned iterator <i>must</i> explicitly be {@link close}'d when it is no longer needed!
         *
         * @param order The order in which the elements of this set will be returned (ascending or descending)
         * @return an iterator over the elements in this set
         */
        public SQLiteMapEntryIterator iterator(final IterationOrder order) {
            return SQLiteMap.this.iterator(order);
        }
    }

    // ======================================================================
    // Constructor
    // ======================================================================

    static {
        try {
            Class.forName(SQLITE_JDBC_DRIVER);
            final Driver driver = Objects.requireNonNull(DriverManager.getDriver(SQLITE_JDBC_PREFIX), "Failed to obtain the SQLite JDBC driver instance from driver manager!");
            if ((driver.getMajorVersion() != 3) || (driver.getMinorVersion() < 36)) {
                throw new AssertionError(String.format("SQLite JDBC driver appears to be an unsupported version! [version: %d.%d, min. required: 3.36]", driver.getMajorVersion(), driver.getMinorVersion()));
            }
        } catch (final Throwable e) {
            throw new LinkageError("Failed to initialize the SQLite JDBC driver!", e);
        }
    }

    private SQLiteMap(final Type<K> typeK, final Type<V> typeV) {
        this(typeK, typeV, null, null, false, true, false);
    }

    private SQLiteMap(final Type<K> typeK, final Type<V> typeV, final Path dbFilePath, final String dbTableName, final boolean truncate, final boolean dropTable, final boolean deleteFile) {
        this.typeK = Objects.requireNonNull(typeK, "typeK");
        this.typeV = Objects.requireNonNull(typeV, "typeV");
        this.dbFilePath = dbFilePath;
        this.dropTable = dropTable;
        this.deleteFile = deleteFile;
        this.dbTableName = createTableName(this, dbTableName);
        try {
            connection = DriverManager.getConnection(SQLITE_JDBC_PREFIX.concat((dbFilePath != null) ? dbFilePath.toString() : SQLITE_INMEMORY_DB));
        } catch (final SQLException e) {
            throw new SQLiteMapException("Failed to create SQLite connection!", e);
        }
        try {
            createSQLiteTable(truncate, dbFilePath != null);
        } catch (final Exception e) {
            try {
                close(); /*avoid resource leak on exception in constructor!*/
            } catch (Exception e2) { }
            throw new SQLiteMapException("Initialization of SQLite table has failed!", e);
        }
    }

    // ======================================================================
    // Public Static Methods
    // ======================================================================

    /**
     * Creates a new <b>{@code SQLiteMap}</b> instance that is backed by an in-memory database.
     * <p>
     * The {@code SQLiteMap} returned by this method operates on a <i>unique</i> database table and drops that table when calling {@link close}.
     *
     * @param keyType the type of the keys stored in the map
     * @param valueType the type of the values stored in the map
     * @return the new {@link SQLiteMap} instance
     */
    public static <K,V> SQLiteMap<K,V> fromMemory(final Class<K> keyType, final Class<V> valueType) {
        return new SQLiteMap<K, V>(typeOf(keyType), typeOf(valueType));
    }

    /**
     * Creates a new <b>{@code SQLiteMap}</b> instance that is backed by a <i>temporary</i> file.
     * <p>
     * The {@code SQLiteMap} returned by this method operates on a <i>unique</i> database file (located in the user's {@code TEMP} directory) and deletes that file when calling {@link close}.
     *
     * @param keyType the type of the keys stored in the map
     * @param valueType the type of the values stored in the map
     * @return the new {@link SQLiteMap} instance
     */
    public static <K,V> SQLiteMap<K,V> fromFile(final Class<K> keyType, final Class<V> valueType) {
        return new SQLiteMap<K, V>(typeOf(keyType), typeOf(valueType), createTemporaryFile(), null, false, true, true);
    }

    /**
     * Creates a new <b>{@code SQLiteMap}</b> instance that is backed by a local database file.
     * <p>
     * The {@code SQLiteMap} returned by this method does <b>not</b> truncate the existing database table and does <b>not</b> drop the table when calling {@link close}.
     *
     * @param keyType the type of the keys stored in the map
     * @param valueType the type of the values stored in the map
     * @param path the path of the database file that will be used to store the map
     * @param tableName the name of the database table that will be used to store key-value pairs
     * @return the new {@link SQLiteMap} instance
     */
    public static <K,V> SQLiteMap<K,V> fromFile(final Class<K> keyType, final Class<V> valueType, final Path path, final String tableName) {
        return fromFile(keyType, valueType, path, tableName, false, false);
    }

    /**
     * Creates a new <b>{@code SQLiteMap}</b> instance that is backed by a local database file.
     * <p>
     * The {@code SQLiteMap} returned by this method <i>optionally</i> truncates the existing database table and <i>optionally</i> drops the table when calling {@link close}.
     *
     * @param keyType the type of the keys stored in the map
     * @param valueType the type of the values stored in the map
     * @param path the path of the database file that will be used to store the map
     * @param tableName the name of the database table that will be used to store key-value pairs
     * @param truncate if {@code true}, initially drop all existing key-value pairs from the table (if the table already exists)
     * @param temporary if {@code true}, the table will be dropped when calling {@link close}
     * @return the new {@link SQLiteMap} instance
     */
    public static <K,V> SQLiteMap<K,V> fromFile(final Class<K> keyType, final Class<V> valueType, final Path path, final String tableName, final boolean truncate, final boolean temporary) {
        Objects.requireNonNull(path, "Path must not be null!");
        Objects.requireNonNull(tableName, "Table name must not be null!");
        return new SQLiteMap<K, V>(typeOf(keyType), typeOf(valueType), path, tableName, truncate, temporary, false);
    }

    /**
     * Returns the <i>major</i>, <i>minor</i> and <i>patch</i> version of the {@code SQLiteMap} library.
     *
     * @return the {@code short[]} array consisting of {@code { VERSION_MAJOR, VERSION_MINOR, VERSION_PATCH }}
     */
    public static short[] getVersion() {
        return new short[] { VERSION_MAJOR, VERSION_MINOR, VERSION_PATCH };
    }

    // ======================================================================
    // Public Instance Methods
    // ======================================================================

    /**
     * Returns the type of keys maintained by this map.
     *
     * @return the type of keys maintained by this map
     */
    public Class<K> getKeyType() {
        return typeK.getUnderlyingType();
    }

    /**
     * Returns the type of mapped values.
     *
     * @return the type of mapped values
     */
    public Class<V> getValueType() {
        return typeV.getUnderlyingType();
    }

    /**
     * Returns the path of the underlying SQLite database.
     *
     * @return if this map is backed by a file-based database, the path of the underlying SQLite database; otherwise {@code null}
     */
    public Path getPath() {
        return dbFilePath;
    }

    /**
     * Returns the name of the underlying SQLite database table.
     *
     * @return the name of the underlying SQLite database table.
     */
    public String getTableName() {
        return dbTableName;
    }

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

    /**
     * A version of {@link containsKey} that tests <i>multiple</i> keys.
     * <p>
     * This method is a performance optimization and should be preferred whenever <i>multiple</i> keys need to be tested.
     *
     * @param keys collection of key whose presence in this map is to be tested
     * @return {@code true} if this map contains mappings for all the specified keys
     */
    public boolean containsAllKeys(final Collection<?> keys) {
        Objects.requireNonNull(keys, "Collection must not be null!");
        ensureConnectionNotClosed();
        try {
            return transaction(connection, () -> {
                for (final Object key : keys) {
                    final K typedKey = typeK.fromObject(key);
                    if ((typedKey == null) || (countKeys(typedKey) <= 0L)) {
                        return false;
                    }
                }
                return true;
            });
        } catch(final Exception e) {
            throw new SQLiteMapException("Failed to determine if keys exist!", e);
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

    /**
     * A version of {@link containsValue} that tests <i>multiple</i> values.
     * <p>
     * This method is a performance optimization and should be preferred whenever <i>multiple</i> values need to be tested.
     *
     * @param values collection of values whose presence in this map is to be tested
     * @return {@code true} if this map contains mappings for all the specified values
     */
    public boolean containsAllValues(final Collection<?> values) {
        Objects.requireNonNull(values, "Collection must not be null!");
        ensureConnectionNotClosed();
        try {
            return transaction(connection, () -> {
                for (final Object value : values) {
                    final V typedValue = typeV.fromObject(value);
                    if ((typedValue == null) || (countValues(typedValue) <= 0L)) {
                        return false;
                    }
                }
                return true;
            });
        } catch(final Exception e) {
            throw new SQLiteMapException("Failed to determine if values exist!", e);
        }
    }

    /**
     * Tests whether this map contains the specified entry.
     *
     * @param o key-value pair whose presence in this map is to be tested
     * @return {@code true} if this map contains the specified key-value pair
     */
    public boolean containsEntry(final Object o) {
        if (!(o instanceof Entry)) {
            return false;
        }
        ensureConnectionNotClosed();
        try {
            final Entry<?, ?> entry;
            final K key = typeK.fromObject((entry = (Entry<?, ?>)o).getKey());
            if (key != null) {
                return typeV.equals(fetchEntry(key), entry.getValue());
            }
            return false;
        } catch (final Exception e) {
            throw new SQLiteMapException("Failed to determine if entry exists!", e);
        }
    }

    /**
     * A version of {@link containsEntry} that tests <i>multiple</i> entries.
     * <p>
     * This method is a performance optimization and should be preferred whenever <i>multiple</i> entries need to be tested.
     *
     * @param entries collection of entries whose presence in this map is to be tested
     * @return {@code true} if this map contains all the specified entries
     */
    public boolean containsAllEntries(final Collection<?> entries) {
        Objects.requireNonNull(entries, "Collection must not be null!");
        ensureConnectionNotClosed();
        try {
            return transaction(connection, () -> {
                for (final Object o : entries) {
                    if (!(o instanceof Entry)) {
                        return false;
                    }
                    final Entry<?, ?> entry;
                    final K key = typeK.fromObject((entry  = (Entry<?, ?>)o).getKey());
                    if ((key == null) || (!typeV.equals(fetchEntry(key), entry.getValue()))) {
                        return false;
                    }
                }
                return true;
            });
        } catch(final Exception e) {
            throw new SQLiteMapException("Failed to determine if entries exist!", e);
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
     * A version of {@link put} that does <b>not</b> return the previous value.
     * <p>
     * This method is a performance optimization and should be preferred whenever the previous value is <b>not</b> needed.
     *
     * @param key key with which the specified value is to be associated
     * @param value value to be associated with the specified key
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
     * A version of {@link putIfAbsent} that does <b>not</b> return the previous value.
     * <p>
     * This method is a performance optimization and should be preferred whenever the previous value is <b>not</b> needed.
     *
     * @param key key with which the specified value is to be associated
     * @param value value to be associated with the specified key
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
     * Add <i>multiple</i> key-value pairs to the map, using a "batch" insert operation.
     * <p>
     * This method is a performance optimization and should be preferred whenever <i>multiple</i> key-value pairs need to be inserted.
     *
     * @param entries a collection containing the key-value pairs to be added to the map
     * @return {@code true} if the key set of this map changed as a result of the call
     */
    public boolean putAll(final Collection<? extends Entry<? extends K, ? extends V>> entries) {
        Objects.requireNonNull(entries, "Collection must not be null!");
        ensureConnectionNotClosed();
        try {
            return transaction(connection, () -> {
                boolean result = false;
                for (final Entry<? extends K, ? extends V> entry : entries) {
                    final K key = entry.getKey();
                    if (countKeys(key) <= 0) {
                        result = true;
                    }
                    upsertEntry(key, entry.getValue());
                }
                return result;
            });
        } catch (final Exception e) {
            throw new SQLiteMapException("Failed to store new key-value pairs!", e);
        }
    }

    /**
     * Add <i>multiple</i> key-value pairs to the map, using a "batch" insert operation.
     * <p>
     * This method is a performance optimization and should be preferred whenever <i>multiple</i> key-value pairs need to be inserted.
     *
     * @param keys a collection containing the keys to be added to the map
     * @param value value to be associated with the specified keys
     * @return {@code true} if the key set of this map changed as a result of the call
     */
    public boolean putAll(final Collection<? extends K> keys, final V value) {
        Objects.requireNonNull(keys, "Keys must not be null!");
        Objects.requireNonNull(value, "Value must not be null!");
        ensureConnectionNotClosed();
        try {
            return transaction(connection, () -> {
                boolean result = false;
                for (final K key : keys) {
                    if (countKeys(key) <= 0) {
                        result = true;
                    }
                    upsertEntry(key, value);
                }
                return result;
            });
        } catch (final Exception e) {
            throw new SQLiteMapException("Failed to store new key-value pairs!", e);
        }
    }

    /**
     * Add <i>multiple</i> key-value pairs to the map, using a "batch" insert operation.
     * <p>
     * This method is a performance optimization and should be preferred whenever <i>multiple</i> key-value pairs need to be inserted.
     *
     * @param entries a collection containing the key-value pairs to be added to the map
     */
    public void putAll0(final Collection<? extends Entry<? extends K, ? extends V>> entries) {
        Objects.requireNonNull(entries, "Collection must not be null!");
        ensureConnectionNotClosed();
        try {
            transaction(connection, () -> upsertAllEntries(entries));
        } catch (final Exception e) {
            throw new SQLiteMapException("Failed to store new key-value pairs!", e);
        }
    }

    /**
     * Add <i>multiple</i> key-value pairs to the map, using a "batch" insert operation.
     * <p>
     * This method is a performance optimization and should be preferred whenever <i>multiple</i> key-value pairs need to be inserted.
     *
     * @param keys a collection containing the keys to be added to the map
     * @param value value to be associated with the specified keys
     */
    public void putAll0(final Collection<? extends K> keys, final V value) {
        Objects.requireNonNull(keys, "Keys must not be null!");
        Objects.requireNonNull(value, "Value must not be null!");
        ensureConnectionNotClosed();
        try {
            transaction(connection, () -> upsertAllEntries(keys, value));
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
        Objects.requireNonNull(newValue, "New value must not be null!");
        if (oldValue == null) {
            return false;
        }
        ensureConnectionNotClosed();
        try {
            return updateEntry(key, oldValue, newValue);
        } catch (final Exception e) {
            throw new SQLiteMapException("Failed to store new key-value pair!", e);
        }
    }

    /**
     * A version of {@link replace} that does <b>not</b> return the previous value.
     * <p>
     * This method is a performance optimization and should be preferred whenever the previous value is <b>not</b> needed.
     *
     * @param key key with which the specified value is associated
     * @param value value to be associated with the specified key
     * @return returns {@code true}, if an existing mapping for the specified key was updated
     */
    public boolean replace0(final K key, final V value) {
        Objects.requireNonNull(key, "Key must not be null!");
        Objects.requireNonNull(value, "Value must not be null!");
        ensureConnectionNotClosed();
        try {
            return updateEntry(key, value);
        } catch (final Exception e) {
            throw new SQLiteMapException("Failed to store new key-value pair!", e);
        }
    }

    @Override
    public V remove(final Object key) {
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
        final V typedValue = typeV.fromObject(value);
        if (typedValue == null) {
            return false;
        }
        ensureConnectionNotClosed();
        try {
            return removeEntry(typedKey, typedValue);
        } catch (final Exception e) {
            throw new SQLiteMapException("Failed to remove key-value pair!", e);
        }
    }

    /**
     * A version of {@link remove} that does <b>not</b> return a value.
     * <p>
     * This method is a performance optimization and should be preferred whenever the previous value is <b>not</b> needed.
     *
     * @param key key whose mapping is to be removed from the map
     * @return returns {@code true}, if the mapping for the specified key was removed
     */
    public boolean remove0(Object key) {
        final K typedKey = typeK.fromObject(key);
        if (typedKey == null) {
            return false;
        }
        ensureConnectionNotClosed();
        try {
            return removeEntry(typedKey);
        } catch (final Exception e) {
            throw new SQLiteMapException("Failed to remove key-value pair!", e);
        }
    }

    /**
     * A version of {@link remove} that removes <i>multiple</i> keys.
     * <p>
     * This method is a performance optimization and should be preferred whenever <i>multiple</i> keys need to be removed.
     *
     * @param keys collection of keys whose mapping is to be removed from the map
     * @return {@code true} if at least one value was removed
     */
    public boolean removeAll(final Collection<?> keys) {
        Objects.requireNonNull(keys, "Collection must not be null!");
        ensureConnectionNotClosed();
        try {
            return transaction(connection, () -> {
                boolean result = false;
                for (final Object key : keys) {
                    final K typedKey = typeK.fromObject(key);
                    if ((typedKey != null) && removeEntry(typedKey)) {
                        result = true;
                    }
                }
                return result;
            });
        } catch (final Exception e) {
            throw new SQLiteMapException("Failed to remove key-value pairs!", e);
        }
    }

    @Override
    public void clear() {
        ensureConnectionNotClosed();
        ++modifyCount;
        final PreparedStatement preparedStatement = sqlClearEntries.getInstance();
        try {
            preparedStatement.executeUpdate();
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
        try {
            transaction(connection, () -> {
                try (final SQLiteMapEntryIterator iterator = iterator()) {
                    while(iterator.hasNext()) {
                        final Entry<K, V> entry = iterator.next();
                        action.accept(entry.getKey(), entry.getValue());
                    }
                }
            });
        } catch (final Exception e) {
            throw new SQLiteMapException("Failed to iterate all key-value pairs!", e);
        }
    }

    @Override
    public void forEach(final Consumer<? super Entry<K, V>> action) {
        Objects.requireNonNull(action);
        try {
            transaction(connection, () -> {
                try (final SQLiteMapEntryIterator iterator = iterator()) {
                    while(iterator.hasNext()) {
                        action.accept(iterator.next());
                    }
                }
            });
        } catch (final Exception e) {
            throw new SQLiteMapException("Failed to iterate all key-value pairs!", e);
        }
    }

    /**
     * Performs the given action for each key in this map.
     *
     * @param action the action to be performed for each key
     */
    public void forEachKey(final Consumer<? super K> action) {
        Objects.requireNonNull(action);
        try {
            transaction(connection, () -> {
                try (final SQLiteMapKeyIterator iterator = keyIterator()) {
                    while(iterator.hasNext()) {
                        action.accept(iterator.next());
                    }
                }
            });
        } catch (final Exception e) {
            throw new SQLiteMapException("Failed to iterate all key-value pairs!", e);
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
     * A version of {@link size} that returns a <b>long</b> value.
     * <p>
     * Unlike {@link size}, <i>this</i> method works correctly with maps containing more than {@code Integer.MAX_VALUE} elements.
     *
     * @return the number of key-value pairs in this map
     */
    public long sizeLong() {
        ensureConnectionNotClosed();
        try {
            final long size = countEntries();
            if (size < 0) {
                throw new IllegalStateException("Entry count could not be determined!");
            }
            return size;
        } catch (final Exception e) {
            throw new SQLiteMapException("Failed to compute new size!", e);
        }
    }

    @Override
    public int size() {
        return Math.toIntExact(sizeLong());
    }

    /**
     * Returns an iterator over the key-value pairs in this map. The key-value pairs are returned in <i>default</i> order.
     * <p>
     * <b>Important notice:</b> The returned iterator <i>must</i> explicitly be {@link close}'d when it is no longer needed!
     *
     * @return an iterator over the key-value pairs in this map
     */
    @Override
    public SQLiteMapEntryIterator iterator() {
        return iterator(defaultKeyOrder);
    }

    /**
     * Returns an iterator over the key-value pairs in this map. The key-value pairs are returned in the specified order.
     * <p>
     * <b>Important notice:</b> The returned iterator <i>must</i> explicitly be {@link close}'d when it is no longer needed!
     *
     * @param order The <i>key</i> order in which the key-value pairs will be returned (ascending or descending)
     * @return an iterator over the key-value pairs in this map
     */
    public SQLiteMapEntryIterator iterator(final IterationOrder order) {
        Objects.requireNonNull(order, "Order must not be null!");
        ensureConnectionNotClosed();
        final PreparedStatement preparedStatement = getStatementByOrder(order, sqlFetchEntries, sqlSortEntries0, sqlSortEntries1);
        try {
            return new SQLiteMapEntryIterator(preparedStatement);
        } catch (final IllegalStateException e) {
            throw e; /* do not intercept IllegalStateException */
        } catch (final Exception e) {
            throw new SQLiteMapException("Failed to query the existing key-value pairs!", e);
        }
    }

    /**
     * Returns an iterator over the keys in this map. The keys are returned in <i>default</i> order.
     * <p>
     * <b>Important notice:</b> The returned iterator <i>must</i> explicitly be {@link close}'d when it is no longer needed!
     *
     * @return an iterator over the keys in this map
     */
    public SQLiteMapKeyIterator keyIterator() {
        return keyIterator(defaultKeyOrder);
    }

    /**
     * Returns an iterator over the keys in this map. The keys are returned in the specified order.
     * <p>
     * <b>Important notice:</b> The returned iterator <i>must</i> explicitly be {@link close}'d when it is no longer needed!
     *
     * @param order The order in which the keys will be returned (ascending or descending)
     * @return an iterator over the keys in this map
     */
    public SQLiteMapKeyIterator keyIterator(final IterationOrder order) {
        Objects.requireNonNull(order, "Order must not be null!");
        ensureConnectionNotClosed();
        final PreparedStatement preparedStatement = getStatementByOrder(order, sqlFetchKeys, sqlSortKeys0, sqlSortKeys1);
        try {
            return new SQLiteMapKeyIterator(preparedStatement);
        } catch (final IllegalStateException e) {
            throw e; /* do not intercept IllegalStateException */
        } catch (final Exception e) {
            throw new SQLiteMapException("Failed to query the existing keys!", e);
        }
    }

    /**
     * Returns an iterator over the values in this map. The values are returned in <i>default</i> order.
     * <p>
     * <b>Important notice:</b> The returned iterator <i>must</i> explicitly be {@link close}'d when it is no longer needed!
     *
     * @return an iterator over the values in this map
     */
    public SQLiteMapValueIterator valueIterator() {
        return valueIterator(defaultValueOrder);
    }

    /**
     * Returns an iterator over the values in this map. The values are returned in the specified order.
     * <p>
     * <b>Important notice:</b> The returned iterator <i>must</i> explicitly be {@link close}'d when it is no longer needed!
     *
     * @param order The order in which the values will be returned (ascending or descending)
     * @return an iterator over the values in this map
     */
    public SQLiteMapValueIterator valueIterator(final IterationOrder order) {
        Objects.requireNonNull(order, "Order must not be null!");
        ensureConnectionNotClosed();
        final PreparedStatement preparedStatement = getStatementByOrder(order, sqlFetchValues, sqlSortValues0, sqlSortValues1);
        try {
            return new SQLiteMapValueIterator(preparedStatement);
        } catch (final IllegalStateException e) {
            throw e; /* do not intercept IllegalStateException */
        } catch (final Exception e) {
            throw new SQLiteMapException("Failed to query the existing keys!", e);
        }
    }

    /**
     * Create an index on the "value" column or drop the existing index. The index created by this method does <b>not</b> have
     * an effect on the "key" column.
     * <p>
     * An index on the "value" column can significantly speed up all <b><i>value</i></b> <i>lookup</i> operations (such as
     * {@link #containsValue containsValue()}), but it comes at a certain memory overhead and it may slow down all
     * <i>insert</i> operations (such as {@link #put put()} or {@link #put0 put0()}). Furthermore, a "value" index is highly
     * recommended, in case that the <b><i>values</i></b> contained in the map need to be iterated
     * {@link #valueIterator(IterationOrder) <i>in a specific order</i>}. Initially, a new {@code SQLiteMap} instance does
     * <b>not</b> have an index on its "value" column.
     * <p>
     * <b>Note:</b> There <i>always</i> is an implicit index on the "key" column, in order to guarantee the uniqueness of the
     * keys and in order to speed up <b><i>key</i></b> or <b><i>key-to-value</i></b> <i>lookup</i> operations.
     *
     * @param enable if {@code true} creates the index (if not created yet), otherwise drops the existing index (if present)
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

    /**
     * Set the <i>default</i> <b>key</b> iteration order. Also returns the previous default key iteration order.
     * <p>
     * This specifies the iteration order to be used by all iterator methods that do <b>not</b> explicitly take an iteration
     * order as parameter. Iterator methods that <i>do</i> take the iteration order as parameter are unaffected!
     * <p>
     * <b>Note:</b> The default iteration order of a new {@code SQLiteMap} instance is {@link IterationOrder#UNSPECIFIED}.
     *
     * @param order the new default <b>key</b> iteration order
     * @return the previous default <b>key</b> iteration order
     */
    public IterationOrder setDefaultKeyOrder(final IterationOrder order) {
        final IterationOrder oldOrder = defaultKeyOrder;
        defaultKeyOrder = Objects.requireNonNull(order, "Order must not be null!");
        return oldOrder;
    }

    /**
     * Set the <i>default</i> <b>value</b> iteration order. Also returns the previous default value iteration order.
     * <p>
     * This specifies the iteration order to be used by all iterator methods that do <b>not</b> explicitly take an iteration
     * order as parameter. Iterator methods that <i>do</i> take the iteration order as parameter are unaffected!
     * <p>
     * <b>Note:</b> The default iteration order of a new {@code SQLiteMap} instance is {@link IterationOrder#UNSPECIFIED}.
     *
     * @param order the new default <b>value</b> iteration order
     * @return the previous default <b>value</b> iteration order
     */
    public IterationOrder setDefaultValueOrder(final IterationOrder order) {
        final IterationOrder oldOrder = defaultValueOrder;
        defaultValueOrder = Objects.requireNonNull(order, "Order must not be null!");
        return oldOrder;
    }

    @Override
    public boolean equals(final Object o) {
        ensureConnectionNotClosed();
        if (o == this) {
            return true;
        }
        if (!(o instanceof Map)) {
            return false;
        }
        try {
            return transaction(connection, () -> checkEquals((Map<?, ?>)o));
        } catch (final Exception e) {
            throw new SQLiteMapException("Failed to test maps for equality!", e);
        }
    }

    @Override
    public int hashCode() {
        ensureConnectionNotClosed();
        try {
            return transaction(connection, this::computeHash);
        } catch (final Exception e) {
            throw new SQLiteMapException("Failed to compute new hash code!", e);
        }
    }

    /**
     * Close the underlying database connection associated with this map. This method <b>must</b> be
     * called when this {@code SQLiteMap} is no longer needed; otherwise a resource leak will occur!
     */
    @Override
    public void close() {
        if (connection != null) {
            try {
                doFinalCleanUp();
            } catch (final Exception e) {
                throw new SQLiteMapException("Failed to close SQLite connection!", e);
            } finally {
                connection = null;
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

    /**
     * Not currently implemented!
     * @exception UnsupportedOperationException
     */
    @Override
    public Spliterator<Entry<K, V>> spliterator() {
        throw new UnsupportedOperationException();
    }

    // ======================================================================
    // Private Methods
    // ======================================================================

    private void ensureConnectionNotClosed() {
        if (connection == null) {
            throw new IllegalStateException("Connection has already been closed!");
        }
    }

    private void createSQLiteTable(final boolean truncate, final boolean fileBased) {
        try (final Statement statement = connection.createStatement()) {
            statement.executeUpdate(String.format(SQL_JOURNAL_MODE, fileBased ? "WAL" : "MEMORY"));
            statement.executeUpdate(String.format(SQL_CREATE_TABLE, dbTableName, typeK.typeName(), typeV.typeName()));
            if (truncate) {
                statement.executeUpdate(String.format(SQL_CLEAR_ENTRIES, dbTableName));
            }
        } catch (final Exception e) {
            throw new SQLiteMapException("Failed to create SQLitabe table!", e);
        }
        try (final ResultSet result = connection.getMetaData().getColumns(null, null, dbTableName, null)) {
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
        ++modifyCount;
        try (final PreparedStatement preparedStatement = connection.prepareStatement(String.format(SQL_DESTROY_TABLE, dbTableName))) {
            preparedStatement.executeUpdate();
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
        ++modifyCount;
        final PreparedStatement preparedStatement = sqlInsertEntry0.getInstance();
        try {
            typeK.setParameter(preparedStatement, 1, key);
            typeV.setParameter(preparedStatement, 2, value);
            try {
                preparedStatement.executeUpdate();
            } finally {
                preparedStatement.clearParameters();
            }
        } catch (final SQLException e) {
            throw new SQLiteMapException("Failed to insert the new key-value pair!", e);
        }
    }

    private void insertEntryOptional(final K key, final V value) {
        ++modifyCount;
        final PreparedStatement preparedStatement = sqlInsertEntry1.getInstance();
        try {
            typeK.setParameter(preparedStatement, 1, key);
            typeV.setParameter(preparedStatement, 2, value);
            try {
                preparedStatement.executeUpdate();
            } finally {
                preparedStatement.clearParameters();
            }
        } catch (final SQLException e) {
            throw new SQLiteMapException("Failed to insert the new key-value pair!", e);
        }
    }

    private void upsertEntry(final K key, final V value) {
        ++modifyCount;
        final PreparedStatement preparedStatement = sqlUpsertEntry.getInstance();
        try {
            typeK.setParameter(preparedStatement, 1, key);
            typeV.setParameter(preparedStatement, 2, value);
            try {
                preparedStatement.executeUpdate();
            } finally {
                preparedStatement.clearParameters();
            }
        } catch (final SQLException e) {
            throw new SQLiteMapException("Failed to upsert the new key-value pair!", e);
        }
    }

    private void upsertAllEntries(final Collection<? extends Entry<? extends K, ? extends V>> entries) {
        ++modifyCount;
        final PreparedStatement preparedStatement = sqlUpsertEntry.getInstance();
        try {
            int currentBatchSize = 0;
            try {
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
            } finally {
                preparedStatement.clearBatch();
                preparedStatement.clearParameters();
            }
        } catch (final SQLException e) {
            throw new SQLiteMapException("Failed to upsert the new key-value pairs!", e);
        }
    }

    private void upsertAllEntries(final Collection<? extends K> keys, final V value) {
        ++modifyCount;
        final PreparedStatement preparedStatement = sqlUpsertEntry.getInstance();
        try {
            int currentBatchSize = 0;
            try {
                for(final K key : keys) {
                    typeK.setParameter(preparedStatement, 1, key);
                    typeV.setParameter(preparedStatement, 2, value);
                    preparedStatement.addBatch();
                    if (++currentBatchSize >= MAXIMUM_BATCH_SIZE) {
                        preparedStatement.executeBatch();
                        currentBatchSize = 0;
                    }
                }
                if (currentBatchSize > 0) {
                    preparedStatement.executeBatch();
                }
            } finally {
                preparedStatement.clearBatch();
                preparedStatement.clearParameters();
            }
        } catch (final SQLException e) {
            throw new SQLiteMapException("Failed to upsert the new key-value pairs!", e);
        }
    }

    private boolean updateEntry(final K key, final V value) {
        ++modifyCount;
        final PreparedStatement preparedStatement = sqlUpdateEntry0.getInstance();
        try {
            typeK.setParameter(preparedStatement, 2, key);
            typeV.setParameter(preparedStatement, 1, value);
            try {
                return (preparedStatement.executeUpdate() > 0);
            } finally {
                preparedStatement.clearParameters();
            }
        } catch (final SQLException e) {
            throw new SQLiteMapException("Failed to insert the new key-value pair!", e);
        }
    }

    private boolean updateEntry(final K key, final V oldValue, final V newValue) {
        ++modifyCount;
        final PreparedStatement preparedStatement = sqlUpdateEntry1.getInstance();
        try {
            typeK.setParameter(preparedStatement, 2, key);
            typeV.setParameter(preparedStatement, 3, oldValue);
            typeV.setParameter(preparedStatement, 1, newValue);
            try {
                return (preparedStatement.executeUpdate() > 0);
            } finally {
                preparedStatement.clearParameters();
            }
        } catch (final SQLException e) {
            throw new SQLiteMapException("Failed to insert the new key-value pair!", e);
        }
    }

    private boolean removeEntry(final K key) {
        ++modifyCount;
        final PreparedStatement preparedStatement = sqlRemoveEntry0.getInstance();
        try {
            typeK.setParameter(preparedStatement, 1, key);
            try {
                return (preparedStatement.executeUpdate() > 0);
           } finally {
                preparedStatement.clearParameters();
            }
        } catch (final SQLException e) {
            throw new SQLiteMapException("Failed to remove the key-value pair!", e);
        }
    }

    private boolean removeEntry(final K key, final V value) {
        ++modifyCount;
        final PreparedStatement preparedStatement = sqlRemoveEntry1.getInstance();
        try {
            typeK.setParameter(preparedStatement, 1, key);
            typeV.setParameter(preparedStatement, 2, value);
            try {
                return (preparedStatement.executeUpdate() > 0);
           } finally {
                preparedStatement.clearParameters();
            }
        } catch (final SQLException e) {
            throw new SQLiteMapException("Failed to remove the key-value pair!", e);
        }
    }

    private int computeHash() {
        try (final SQLiteMapEntryIterator iter = iterator()) {
            int hashCode = 0;
            while(iter.hasNext()) {
                hashCode += iter.next().hashCode();
            }
            return hashCode;
        }
    }

    private boolean checkEquals(final Map<?, ?> map) {
        final long size =  (map instanceof SQLiteMap) ? ((SQLiteMap<?, ?>)map).sizeLong() : map.size();
        if (countEntries() != size) {
            return false;
        }
        try (final SQLiteMapEntryIterator iter = iterator()) {
            while(iter.hasNext()) {
                final Entry<K, V> current = iter.next();
                final Object value = map.get(current.getKey());
                if ((value == null) || (!typeV.equals(current.getValue(), value))) {
                    return false;
                }
            }
        }
        return true;
    }

    private PreparedStatement getStatementByOrder(final IterationOrder iterationOrder, final PreparedStatementHolder statement0, final PreparedStatementHolder statement1, final PreparedStatementHolder statement2) {
        switch (iterationOrder) {
        case UNSPECIFIED:
            return Objects.requireNonNull(statement0).getInstance();
        case ASCENDING:
            return Objects.requireNonNull(statement1).getInstance();
        case DESCENDING:
            return Objects.requireNonNull(statement2).getInstance();
        default:
            throw new IllegalArgumentException("Invalid iteration order specified!");
        }
    }

    private void doFinalCleanUp() throws SQLException, IOException {
        try {
            try {
                final List<AutoCloseable> reverse = new ArrayList<AutoCloseable>(cleanUpQueue);
                Collections.reverse(reverse);
                for (final AutoCloseable resource : reverse) {
                    try {
                        resource.close();
                    } catch (Exception e) { }
                }
            } finally {
                if (dropTable) {
                    destroySQLiteTable();
                }
            }
        } finally {
            connection.close();
            if ((dbFilePath != null) && deleteFile) {
                Files.delete(dbFilePath);
            }
        }
    }

    // --------------------------------------------------------
    // Utility Methods
    // --------------------------------------------------------

    private static String createTableName(final Object instance, final String name) {
        final String dbTableName = (name != null) ? name.trim() : EMPTY_STRING;
        if ((!dbTableName.isEmpty()) && (!REGEX_TABLE_NAME.matcher(dbTableName).matches())) {
            throw new IllegalArgumentException("Illegal table name! ['" + name + "']");
        }
        return SQLITE_NAME_PREFIX.concat(dbTableName.isEmpty() ? String.format("%08x", System.identityHashCode(instance)) : dbTableName);
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

    private static void transaction(final Connection connection, final Runnable runnable) throws SQLException {
        transaction(connection, () -> {
            runnable.run();
            return (Void) null;
        });
    }

    private static <T> T transaction(final Connection connection, final Callable<T> callable) throws SQLException {
        if (!connection.getAutoCommit()) {
            throw new IllegalStateException("Transaction already in progress!");
        }
        connection.setAutoCommit(false);
        try {
            final T result = callable.call();
            connection.commit();
            return result;
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
    }

    private static Path createTemporaryFile() {
        try {
            return Files.createTempFile("sqlitemap-", ".db");
        } catch (Exception e) {
            throw new SQLiteMapException("Failed to create temporary file!", e);
        }
    }
}
