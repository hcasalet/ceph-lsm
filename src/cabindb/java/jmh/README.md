# JMH Benchmarks for CabinJava

These are micro-benchmarks for CabinJava functionality, using [JMH (Java Microbenchmark Harness)](https://openjdk.java.net/projects/code-tools/jmh/).

## Compiling

**Note**: This uses a specific build of CabinDB that is set in the `<version>` element of the `dependencies` section of the `pom.xml` file. If you are testing local changes you should build and install a SNAPSHOT version of cabindbjni, and update the `pom.xml` of cabindbjni-jmh file to test with this.

```bash
$ mvn package
```

## Running
```bash
$ java -jar target/cabindbjni-jmh-1.0-SNAPSHOT-benchmarks.jar
```

NOTE: you can append `-help` to the command above to see all of the JMH runtime options.
