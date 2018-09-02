# Kafka Journal [![Build Status](https://travis-ci.org/evolution-gaming/kafka-journal.svg)](https://travis-ci.org/evolution-gaming/kafka-journal) [![Coverage Status](https://coveralls.io/repos/evolution-gaming/kafka-journal/badge.svg)](https://coveralls.io/r/evolution-gaming/kafka-journal) [![Codacy Badge](https://api.codacy.com/project/badge/Grade/fab03059b5f94fa5b1e7ad7bddfe8b07)](https://www.codacy.com/app/evolution-gaming/kafka-journal?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=evolution-gaming/kafka-journal&amp;utm_campaign=Badge_Grade) [ ![version](https://api.bintray.com/packages/evolutiongaming/maven/kafka-journal/images/download.svg) ](https://bintray.com/evolutiongaming/maven/kafka-journal/_latestVersion) [![License: MIT](https://img.shields.io/badge/License-MIT-yellowgreen.svg)](https://opensource.org/licenses/MIT)

# >>> Not for production use. yet. <<<

This library provides ability to use [kafka](https://kafka.apache.org) as storage for events.
Kafka is a perfect fit in case you want to have streaming capabilities for your events
However it also uses [cassandra](http://cassandra.apache.org) to keep data access performance on acceptable level and overcome kafka `retention policy` 
Cassandra is a default choice, but you may use any other storage which satisfies following interfaces:
* Read side, called within client library: [EventualJournal](journal/src/main/scala/com/evolutiongaming/kafka/journal/eventual/EventualJournal.scala) 
* Write side, called from replicator app: [ReplicatedJournal](journal/src/main/scala/com/evolutiongaming/kafka/journal/eventual/ReplicatedJournal.scala) 

## High level idea

### Writing events flow:

1. Journal client publishes events to kafka
2. Replicator app stores events to cassandra


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
* It is easy to replace cassandra here with some relational database


## State recovery performance

Reading events performance depends on finding the closest offset to the marker as well on replication latency (time difference between the moment event has been written to kafka and the moment when event gets into cassandra)

We may share same kafka consumer for many simultaneous recoveries


## Read & write capabilities:
* Client allowed to `read` + `write` kafka and `read` cassandra
* Replicator allowed to `read` kafka and `read` + `write` cassandra

Hence we recommend to configure access rights accordingly.


## Akka persistence plugin

In order to use kafka-journal as [akka persistence plugin](https://doc.akka.io/docs/akka/2.5/persistence.html#storage-plugins) you would need to add following to your `*.conf` file:

```hocon
akka.persistence.journal.plugin = "evolutiongaming.kafka-journal.persistence.journal"
```

Unfortunately akka persistence `snapshot` plugin is not implemented yet.


## Setup

```scala
resolvers += Resolver.bintrayRepo("evolutiongaming", "maven")

libraryDependencies += "com.evolutiongaming" %% "kafka-journal" % "0.0.1"

libraryDependencies += "com.evolutiongaming" %% "kafka-journal-persistence" % "0.0.1"

libraryDependencies += "com.evolutiongaming" %% "kafka-journal-replicator" % "0.0.1"

```
