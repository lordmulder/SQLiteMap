# SQLiteMap &ndash; a map implementation based on SQLite

The **`SQLiteMap`** class provides a map implementation that is backed by an [SQLite](https://www.sqlite.org/) database. It can employ an "in-memory" database or a local database file. Compared to Java's standard `HashMap` class, the *"in-memory"* variant of `SQLiteMap` is better suited for very large maps; it has a smaller memory footprint and does **not** clutter the Java heap space. The *file-based* variant of `SQLiteMap` can handle even bigger maps and provides full persistence.


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

&#128161; Please see the **documentation** (Javadoc) for details!


## Dependencies

`SQLiteMap` is compatible with JDK 8 or later. It requires the [**SQLite JDBC Driver**](https://mvnrepository.com/artifact/org.xerial/sqlite-jdbc) to be available.


## License

`SQLiteMap` was created by LoRd_MuldeR [&lt;mulder2@gmx.de&gt;](mailto:mulder2@gmx.de).

To the extent possible under law, the person who associated **CC0** with `SQLiteMap` has waived all copyright and related or neighboring rights to `SQLiteMap`. You should have received a copy of the **CC0** legalcode along with this work.

If not, please refer to:  
<http://creativecommons.org/publicdomain/zero/1.0/>
