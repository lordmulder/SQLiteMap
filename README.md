# SQLiteMap &ndash; a map implementation based on SQLite

![JDK8](https://img.shields.io/badge/JDK-8%2B-blue) ![SQLite3](https://img.shields.io/badge/SQLite-3.36-blue) ![JUnit5](https://img.shields.io/badge/JUnit-5.8-blue) ![GitHub all releases](https://img.shields.io/github/downloads/lordmulder/sqlitemap/total)

The **`SQLiteMap`** class provides a [`Map`](https://docs.oracle.com/javase/8/docs/api/java/util/Map.html) implementation that is backed by an [*SQLite*](https://www.sqlite.org/) database. It can employ an "in-memory" database as well as a local database file. Compared to Java's standard `HashMap` class, the "in-memory" variant of `SQLiteMap` is better suited for *very large* maps; it has a smaller memory footprint and it does **not** clutter the Java heap space. The file-based variant of `SQLiteMap` provides full persistence.

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

In order to build `SQLiteMap` from the sources, [**JDK 17**](https://adoptium.net/releases.html?variant=openjdk17) and [**Apache Ant**](https://ant.apache.org/), version 1.10.x (or later), are required!

Make sure that the environment variables `JAVA_HOME` and `ANT_HOME` are pointing to the JDK 17 home directory and the Ant home directory, respectively. Also make sure that [**ant**](http://manpages.ubuntu.com/manpages/trusty/man1/ant.1.html) and [**git**](https://git-scm.com/docs/git) can be found in your `PATH`.

The provided *Apache Ant* buildfile (`build.xml`) requires [**Apache Commons BCEL**](https://mvnrepository.com/artifact/org.apache.bcel/bcel) to be available in the Ant's classpath! A simple way to achieve this is copying the required JAR file into your `${ANT_HOME}/lib` directory.

Finally, the environment variable `JDK8_BOOT_PATH` needs to be pointing to the `rt.jar` library from [**JDK 8**](https://adoptium.net/releases.html?variant=openjdk8).


### *Test* dependencies:

In order to compile the *JUnit* tests, the [**JUnit Jupiter API**](https://mvnrepository.com/artifact/org.junit.jupiter/junit-jupiter-api/), version 5.x, needs to be present in the `lib` directory!


## Building and Testing

### Building the library:

In order to build the `SQLiteMap` library, simply run **`ant`** from the project's base directory:

    $ cd ~/workspace/sqlitemap
    $ ant

The *default* target `all` compiles the source code (except tests), generates the Javadoc and builds the JAR files.

### Running the JUnit tests:

In order to run the [**JUnit 5**](https://junit.org/junit5/) tests, the test classes need to be *compiled* first:

    $ ant clean compile-test

Then the [JUnit Console Launcher](https://mvnrepository.com/artifact/org.junit.platform/junit-platform-console-standalone) may be invoked from the project's base directory, for example:

    $ java -jar junit-platform-console-standalone.jar -cp lib/sqlite-jdbc-<version>.jar -cp bin \
        --include-classname='(.*)SQLite(.+)Test$' --scan-classpath


## Source Code

The source code is available from the "official" Git mirrors:

| Mirror     | URL                                                        | Browse                                                        |
| ---------- | ---------------------------------------------------------- | --------------------------------------------------------------|
| GitHub     | `git clone https://github.com/lordmulder/SQLiteMap.git`    | [&#128279; link](https://github.com/lordmulder/SQLiteMap)     |
| Bitbucket  | `git clone https://bitbucket.org/muldersoft/sqlitemap.git` | [&#128279; link](https://bitbucket.org/muldersoft/sqlitemap/) |
| GitLab     | `git clone https://gitlab.com/lord_mulder/sqlitemap.git  ` | [&#128279; link](https://gitlab.com/lord_mulder/sqlitemap)    |
| repo.or.cz | `git clone https://repo.or.cz/sqlitemap.git`               | [&#128279; link](https://repo.or.cz/sqlitemap.git)            |


## Download

Download the latest *binary* distribution here:  
**<https://github.com/lordmulder/SQLiteMap/releases/latest>**


### Code signature:

The following [**OpenPGP key**](http://muldersoft.com/#signkey) is used to sign all "official" releases binaries:

    pub   rsa4096 2015-08-23 [expires: 2025-08-20]
          DFAD 4340 D83D 4020 435D 78BB 2B59 1336 5F57 E03F
    uid   LoRd_MuldeR (muldersoft.com) 

See [*here*](https://www.gnupg.org/gph/en/manual/x135.html) for details on how to verify file signatures using [GnuPG](https://www.gnupg.org/). Windows users should refer to [Gpg4win](https://www.gpg4win.de/).


## License

`SQLiteMap` was created by LoRd_MuldeR [&lt;mulder2@gmx.de&gt;](mailto:mulder2@gmx.de).

To the extent possible under law, the person who associated **CC0** with `SQLiteMap` has waived all copyright and related or neighboring rights to `SQLiteMap`. You should have received a copy of the **CC0** legalcode along with this work.

If not, please refer to:  
<http://creativecommons.org/publicdomain/zero/1.0/>


&nbsp;

[&#128020;](https://www.youtube.com/watch?v=VF9UMona74w)
