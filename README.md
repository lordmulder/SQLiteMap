# SQLiteMap &ndash; a map implementation based on SQLite

The **`SQLiteMap`** class provides a map implementation that is backed by an [SQLite](https://www.sqlite.org/) database. It can employ an "in-memory" database or a local database file. Compared to Java's standard `HashMap` class, the *"in-memory"* variant of `SQLiteMap` is better suited for very large maps; it has a smaller memory footprint and does **not** clutter the Java heap space. The *file-based* variant of `SQLiteMap` can handle even bigger maps and provides full persistence.

`SQLiteMap` is compatible with JDK 8 or later. We recommend using OpenJDK builds provided by [Adoptium.net](https://adoptium.net/).


## Getting started

This example shows how to create and use an "in-memory" **`SQLiteMap`** instance:

    import java.util.Map.Entry;
    import com.muldersoft.container.sqlite.SQLiteMap;
    
    public class Main {
      public static void main(String[] args) throws Exception {
        try (final SQLiteMap<String, String> map = SQLiteMap.fromMemory(String.class, String.class)) {
          map.put0("Foo", "Lorem ipsum dolor sit amet");
          map.put0("Bar", "Consetetur sadipscing elitr");
          for (final Entry<String, String> e : map.entrySet()) {
            System.out.println('"' + e.getKey() + "\" --> \"" + e.getValue() + '"');
          }
        }
      }
    }

&#128073; Please see the included **documentation** (Javadoc) for details!


## Dependencies

### *Runtime* dependencies:

`SQLiteMap` requires the [**SQLite JDBC Driver**](https://mvnrepository.com/artifact/org.xerial/sqlite-jdbc), version 3.36 or newer, to be available in the classpath at runtime!

### *Build* dependencies:

The provided [*Ant*](https://ant.apache.org/) buildfile (`build.xml`) requires [**Apache Commons BCEL**](https://mvnrepository.com/artifact/org.apache.bcel/bcel) to be available in Ant's classpath!

A simple way to achieve this is copying the JAR file into your `%ANT_HOME%/lib` directory.

### *Test* dependencies:

In order to run the tests, the [**JUnit 5**](https://junit.org/junit5/) platform and "junit-jupiter" engine libraries must be available in the classpath!

Please see [*here*](https://ant.apache.org/manual/Tasks/junitlauncher.html) for a list of the required JAR files. These files should be located in the `lib` directory.


## License

`SQLiteMap` was created by LoRd_MuldeR [&lt;mulder2@gmx.de&gt;](mailto:mulder2@gmx.de).

To the extent possible under law, the person who associated **CC0** with `SQLiteMap` has waived all copyright and related or neighboring rights to `SQLiteMap`. You should have received a copy of the **CC0** legalcode along with this work.

If not, please refer to:  
<http://creativecommons.org/publicdomain/zero/1.0/>
