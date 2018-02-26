# Reflow

Reflow is a Java library for building graph-based workflows.

With support for parallel execution of tasks, custom scheduling backends, and
intelligent resumption in the event of a failure, Reflow makes it easy to
coordinate interdependent units of work. We use it internally to drive a variety
of data ingestion and processing.

Check out the [user guide][userguide] for a quick walkthrough.

## Getting started

JARs can be downloaded from [Bintray JCenter][bintray] either manually or
through your build system.

To add a dependency using Gradle:

```
dependencies {
    compile 'com.tripadvisor.reflow:reflow:1.0.1'
}
```

Using Maven:

```
<dependency>
    <groupId>com.tripadvisor.reflow</groupId>
    <artifactId>reflow</artifactId>
    <version>1.0.1</version>
</dependency>
```

## License

Reflow is licensed under the [Apache License, Version 2.0](LICENSE).

Copyright (C) 2018 TripAdvisor LLC

[bintray]: https://bintray.com/tripadvisor/reflow
[userguide]: https://github.com/tripadvisor/reflow/wiki
