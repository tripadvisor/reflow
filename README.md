# Reflow

Reflow is a compact Java library for building graph-based workflows with
checkpointing.

Here are some highlights:

- Declare arbitrary dependency relationships between individual tasks, then run
  the whole workflow with maximal parallelism in a single method call

- Run tasks in a single local thread or across multiple machines

- Skip tasks that have already run when restarting

- Serialize the state of a workflow, deserialize it somewhere else, and keep
  right on trucking--without interrupting running tasks

We use Reflow internally to drive a variety of data ingestion/processing jobs.

## Getting started

We hope to have everything available in Maven Central soon.
JAR files are also available on the [releases page][releases].

Next, check out the [user guide][userguide].

## License

Reflow is licensed under the [Apache License, Version 2.0](LICENSE).

Copyright (C) 2017 TripAdvisor LLC

[userguide]: https://github.com/tripadvisor/reflow/wiki/
[releases]: https://github.com/tripadvisor/reflow/releases/
