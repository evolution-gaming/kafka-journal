# Kafka Journal [![Build Status](https://travis-ci.org/evolution-gaming/kafka-journal.svg)](https://travis-ci.org/evolution-gaming/kafka-journal) [![Coverage Status](https://coveralls.io/repos/evolution-gaming/kafka-journal/badge.svg)](https://coveralls.io/r/evolution-gaming/kafka-journal) [![Codacy Badge](https://api.codacy.com/project/badge/Grade/fab03059b5f94fa5b1e7ad7bddfe8b07)](https://www.codacy.com/app/evolution-gaming/kafka-journal?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=evolution-gaming/kafka-journal&amp;utm_campaign=Badge_Grade) [ ![version](https://api.bintray.com/packages/evolutiongaming/maven/kafka-journal/images/download.svg) ](https://bintray.com/evolutiongaming/maven/kafka-journal/_latestVersion) [![License: MIT](https://img.shields.io/badge/License-MIT-yellowgreen.svg)](https://opensource.org/licenses/MIT)

Kafka journal implementation will also need some additional storage to overcome kafka `retention policy` and prevent the full topic scan in some corner cases. 
But It has relaxed requirements for writes.

# >>> Not for production use. yet. <<<


## High level idea

### Writing events flow:

1 Jurnal client publishes events to kafka
2 Replicator app stores events to cassandra


### Reading events flow: 

1. Client publishes special marker to kafka so we can make sure there are no more events to expect
2. Client reads events from cassandra, however at this point we are not yet sure that all events are replicated from kafka to cassandra
3. Client read events from kafka using offset of last event found in cassandra
4. We consider recovery finished when marker found in kafka


## Notes

* Kafka topic may be used for many different entities
* We don't need to store all events in kafka as long as they are in cassandra
* We do not cover snapshots yet
* Replicator is a separate application
* It is easy to replace cassandra here with some relation database


## State recovery performance

Reading events performance depends on finding the closest offset to the marker as well on replication latency (time difference between the moment event has been written to kafka and the moment when event gets into cassandra)

We may share same kafka consumer for many simultaneous recoveries


## Read & write capabilities:
* Client allowed to `read` + `write` kafka and `read` cassandra
* Replicator allowed to `read` kafka and `read` + `write` cassandra


## Setup

```scala
resolvers += Resolver.bintrayRepo("evolutiongaming", "maven")

libraryDependencies += "com.evolutiongaming" %% "kafka-journal" % "0.0.1"
```
