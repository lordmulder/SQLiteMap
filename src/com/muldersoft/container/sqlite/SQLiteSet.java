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

import java.nio.file.Path;
import java.util.Collection;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;

import com.muldersoft.container.sqlite.SQLiteMap.SQLiteMapException;

/**
 * The {@code SQLiteSet} class provides a set implementation that is backed by an SQLite database. It can employ an "in-memory"
 * database or a local database file. Compared to Java's standard {@code HashSet} class, the "in-memory" variant of
 * {@code SQLiteSet} is better suited for <i>very large</i> sets; it has a smaller memory footprint and does <b>not</b> clutter
 * the Java heap space. The file-based variant of {@code SQLiteSet} can handle even bigger sets and provides full persistence.
 * <p>
 * New instances of {@code SQLiteSet} that are backed by an "in-memory" database or by a file-based database can be created by
 * calling the static method {@link #fromMemory fromMemory()} or {@link #fromFile fromFile()}, respectively. Because
 * {@code SQLiteSet} is backed by an SQLite database, the <i>types</i> supported as elements are restricted. For the time
 * being, <i>only</i> the types {@code String}, {@code Boolean}, {@code Byte}, {@code byte[]}, {@code Integer}, {@code Long},
 * {@code Instant} as well as {@code BigInteger} are supported. Other types may be stored via serialization.
 * <p>
 * This class is <b>not</b> "thread-safe", in the sense that the <i>same</i> instance of {@code SQLiteSet} <b>must not</b> be
 * accessed concurrently by <i>different</i> threads. However, it is perfectly "safe" to created <i>multiple</i> instances of
 * {@code SQLiteSet} in <i>different</i> threads; each instance uses its own separate SQLite connection and its own separate
 * database table. As long as each thread <i>only</i> accesses its own instance an does <i>not</i> share that instance with
 * other threads, <b>no</b> synchronization is required. In case that the <i>same</i> instance of {@code SQLiteSet} needs to be
 * shared across <i>different</i> threads, the application <b>must</b> explicitly <i>synchronize</i> <b>all</b> accesses to
 * that "shared" instance! This includes any iterators, key/entry sets or value collections returned by this class.
 * <p>
 * <b>Important notice:</b> Instances of this class <i>must</i> explicitly be {@link close}'d when they are no longer needed!
 *
 * @author Created by LoRd_MuldeR &lt;mulder2@gmx.de&gt;
 * @param <E> the type of elements maintained by this set
 */
public class SQLiteSet<E> implements Set<E>, AutoCloseable {

    private final SQLiteMap<E, Boolean> map;

    // ======================================================================
    // Exception
    // ======================================================================

    /**
     * Exception class to indicate {@link SQLiteSet} errors
     */
    public static class SQLiteSetException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        public SQLiteSetException(final String message) {
            super(message);
        }

        public SQLiteSetException(final String message, final Throwable cause) {
            super(message, cause);
        }

        public SQLiteSetException(final SQLiteMapException exception) {
            super(exception);
        }
    }

    // ======================================================================
    // Set Iterator
    // ======================================================================

    /**
     * Iterator implementation for iterating @{link SQLiteSet} elements.
     * <p>
     * <b>Important notice:</b> Instances of this class <i>must</i> explicitly be {@link close}'d when they are no longer needed!
     */
    public class SQLiteSetIterator implements Iterator<E>, AutoCloseable {
        private final SQLiteMap<E, Boolean>.SQLiteMapKeyIterator iter;

        public SQLiteSetIterator(final SQLiteMap<E, Boolean>.SQLiteMapKeyIterator iter) {
            try {
                this.iter = Objects.requireNonNull(iter);
            } catch (final SQLiteMapException e) {
                throw new SQLiteSetException(e);
            }
        }

        @Override
        public boolean hasNext() {
            try {
                return iter.hasNext();
            } catch (final SQLiteMapException e) {
                throw new SQLiteSetException(e);
            }
        }

        @Override
        public E next() {
            try {
                return iter.next();
            } catch (final SQLiteMapException e) {
                throw new SQLiteSetException(e);
            }
        }

        @Override
        public void close() throws Exception {
            try {
                iter.close();
            } catch (final SQLiteMapException e) {
                throw new SQLiteSetException(e);
            }
        }
    }

    // ======================================================================
    // Constructor
    // ======================================================================

    private SQLiteSet(final SQLiteMap<E, Boolean> map) {
        this.map = Objects.requireNonNull(map, "Map must not be null!");
    }

    /**
     * Creates a new {@code SQLiteSet} instance that is backed by an in-memory database.
     * <p>
     * The {@code SQLiteSet} returned by this method <i>does</i> drop the table when calling {@link close}.
     *
     * @param elementType the type of the elements stored in the set
     * @return the new {@code SQLSQLiteSetiteSet} instance
     */
    public static <E> SQLiteSet<E> fromMemory(final Class<E> elementType) {
        try {
            return new SQLiteSet<E>(SQLiteMap.fromMemory(elementType, Boolean.class));
        } catch (final SQLiteMapException e) {
            throw new SQLiteSetException(e);
        }
    }

    /**
     * Creates a new {@code SQLiteSet} instance that is backed by a local database file.
     * <p>
     * The {@code SQLiteSet} returned by this method does <b>not</b> drop the table when calling {@link close}.
     *
     * @param elementType the type of the elements stored in the set
     * @param path the path of the database file that will be used to store the set
     * @param tableName the name of the database table that will be used to store elements
     * @return the new SQLiteSet instance
     */
    public static <E> SQLiteSet<E> fromFile(final Class<E> elementType, final Path path, final String tableName) {
        try {
            return new SQLiteSet<E>(SQLiteMap.fromFile(elementType, Boolean.class, path, tableName));
        } catch (final SQLiteMapException e) {
            throw new SQLiteSetException(e);
        }
    }

    /**
     * Creates a new {@code SQLiteSet} instance that is backed by a local database file.
     * <p>
     * The {@code SQLiteSet} returned by this method <i>optionally</i> drops the table when calling {@link close}.
     *
     * @param elementType the type of the elements stored in the set
     * @param path the path of the database file that will be used to store the set
     * @param tableName the name of the database table that will be used to store elements
     * @param truncate if {@code true}, initially drops all existing elements from the table
     * @param temporary if {@code true}, the table will be dropped when calling {@link close}
     * @return the new {@code SQLiteSet} instance
     */
    public static <E> SQLiteSet<E> fromFile(final Class<E> elementType, final Path path, final String tableName, final boolean truncate, final boolean temporary) {
        try {
            return new SQLiteSet<E>(SQLiteMap.fromFile(elementType, Boolean.class, path, tableName, truncate, temporary));
        } catch (final SQLiteMapException e) {
            throw new SQLiteSetException(e);
        }
    }

    // ======================================================================
    // Public Methods
    // ======================================================================

    @Override
    public int size() {
        try {
            return map.size();
        } catch (final SQLiteMapException e) {
            throw new SQLiteSetException(e);
        }
    }

    public long sizeLong() {
        try {
            return map.sizeLong();
        } catch (final SQLiteMapException e) {
            throw new SQLiteSetException(e);
        }
    }

    @Override
    public boolean isEmpty() {
        try {
            return map.isEmpty();
        } catch (final SQLiteMapException e) {
            throw new SQLiteSetException(e);
        }
    }

    @Override
    public boolean contains(final Object o) {
        try {
            return map.containsKey(o);
        } catch (final SQLiteMapException e) {
            throw new SQLiteSetException(e);
        }
    }

    @Override
    public boolean containsAll(final Collection<?> c) {
        try {
            return map.containsAllKeys(c);
        } catch (final SQLiteMapException e) {
            throw new SQLiteSetException(e);
        }
    }

    @Override
    public SQLiteSetIterator iterator() {
        try {
            return new SQLiteSetIterator(map.keyIterator());
        } catch (final SQLiteMapException e) {
            throw new SQLiteSetException(e);
        }
    }

    @Override
    public boolean add(final E element) {
        try {
            return (map.put(element, Boolean.TRUE) == null);
        } catch (final SQLiteMapException e) {
            throw new SQLiteSetException(e);
        }
    }

    /**
     * A version of {@link add} that does <b>not</b> return whether the element was already present.
     * <p>
     * This method is a performance optimization and should be preferred whenever the result is <b>not</b> needed.
     *
     * @param element element to be added to this set
     */
    public void add0(final E element) {
        try {
            map.put0(element, Boolean.TRUE);
        } catch (final SQLiteMapException e) {
            throw new SQLiteSetException(e);
        }
    }

    @Override
    public boolean addAll(final Collection<? extends E> elements) {
        try {
            boolean result = false;
            for (final E element : elements) {
                if (map.put(element, Boolean.TRUE) == null) {
                    result = true;
                }
            }
            return result;
        } catch (final SQLiteMapException e) {
            throw new SQLiteSetException(e);
        }
    }

    /**
     * A version of {@link addAll} that does <b>not</b> return a result.
     * <p>
     * This method is a performance optimization and should be preferred whenever the result is <b>not</b> needed.
     *
     * @param elements collection containing elements to be added to this set
     */
    public void addAll0(final Collection<? extends E> elements) {
        try {
            map.putAll(elements, Boolean.TRUE);
        } catch (final SQLiteMapException e) {
            throw new SQLiteSetException(e);
        }
    }

    @Override
    public boolean remove(final Object o) {
        try {
            return (map.remove(o) != null);
        } catch (final SQLiteMapException e) {
            throw new SQLiteSetException(e);
        }
    }

    /**
     * A version of {@link remove} that does <b>not</b> return whether the set contained the element.
     * <p>
     * This method is a performance optimization and should be preferred whenever the result is <b>not</b> needed.
     *
     * @param o object to be removed from this set, if present
     */
    public void remove0(final Object o) {
        try {
            map.remove0(o);
        } catch (final SQLiteMapException e) {
            throw new SQLiteSetException(e);
        }
    }

    @Override
    public boolean removeAll(final Collection<?> c) {
        try {
            return map.removeAll(c);
        } catch (final SQLiteMapException e) {
            throw new SQLiteSetException(e);
        }
    }

    /**
     * A version of {@link removeAll} that does <b>not</b> return a result.
     * <p>
     * This method is a performance optimization and should be preferred whenever the result is <b>not</b> needed.
     *
     * @param c collection containing elements to be removed from this set
     */
    public void removeAll0(final Collection<?> c) {
        try {
            map.removeAll0(c);
        } catch (final SQLiteMapException e) {
            throw new SQLiteSetException(e);
        }
    }

    @Override
    public void clear() {
        try {
            map.clear();
        } catch (final SQLiteMapException e) {
            throw new SQLiteSetException(e);
        }
    }

    @Override
    public void close() {
        try {
            map.clear();
        } catch (final SQLiteMapException e) {
            throw new SQLiteSetException(e);
        }
    }

    @Override
    public int hashCode() {
        try {
            return map.hashCode();
        } catch (final SQLiteMapException e) {
            throw new SQLiteSetException(e);
        }
    }

    @Override
    public boolean equals(final Object o) {
        try {
            if (o == this) {
                return true;
            } else if (o instanceof SQLiteSet) {
                return map.equals(((SQLiteSet<?>)o).map);
            } else {
                return false;
            }
        } catch (final SQLiteMapException e) {
            throw new SQLiteSetException(e);
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
    public Object[] toArray() {
        throw new UnsupportedOperationException();
    }

    /**
     * Not currently implemented!
     * @exception UnsupportedOperationException
     */
    @Override
    public <T> T[] toArray(T[] a) {
        throw new UnsupportedOperationException();
    }

    /**
     * Not currently implemented!
     * @exception UnsupportedOperationException
     */
    @Override
    public boolean retainAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }
}
