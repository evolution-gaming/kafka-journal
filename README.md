# Kafka Journal [![Build Status](https://travis-ci.org/evolution-gaming/kafka-journal.svg)](https://travis-ci.org/evolution-gaming/kafka-journal) [![Coverage Status](https://coveralls.io/repos/evolution-gaming/kafka-journal/badge.svg)](https://coveralls.io/r/evolution-gaming/kafka-journal) [![Codacy Badge](https://api.codacy.com/project/badge/Grade/fab03059b5f94fa5b1e7ad7bddfe8b07)](https://www.codacy.com/app/evolution-gaming/kafka-journal?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=evolution-gaming/kafka-journal&amp;utm_campaign=Badge_Grade) [ ![version](https://api.bintray.com/packages/evolutiongaming/maven/kafka-journal/images/download.svg) ](https://bintray.com/evolutiongaming/maven/kafka-journal/_latestVersion) [![License: MIT](https://img.shields.io/badge/License-MIT-yellowgreen.svg)](https://opensource.org/licenses/MIT) [![Chat](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/evolution-gaming/kafka-journal)

> Stream data from two sources where one is eventually consistent and the other one loses its tail

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

## Api

```scala
trait Journal[F[_]] {

  def append(key: Key, events: Nel[Event]): F[PartitionOffset]

  def read(key: Key, from: SeqNr): Stream[F, Event]

  def pointer(key: Key): F[Option[SeqNr]]

  def delete(key: Key, to: SeqNr): F[Option[PartitionOffset]]
}
```

## Troubleshooting

### Kafka exceptions in logs

Kafka client is tend to log some exceptions at `error` level, however in reality those are harmless in case of operation retried successfully.
Retriable exceptions usually extend [RetriableException](https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/common/errors/RetriableException.java)

Here is the list of known error logs you may ignore:

* Offset commit failed on partition .. at offset ..: The request timed out.
* Offset commit failed on partition .. at offset ..: The coordinator is loading and hence can't process requests.
* Offset commit failed on partition .. at offset ..: This is not the correct coordinator.
* Offset commit failed on partition .. at offset ..: This server does not host this topic-partition. 


## Akka persistence plugin

In order to use kafka-journal as [akka persistence plugin](https://doc.akka.io/docs/akka/2.5/persistence.html#storage-plugins) you would need to add following to your `*.conf` file:

```hocon
akka.persistence.journal.plugin = "evolutiongaming.kafka-journal.persistence.journal"
```

Unfortunately akka persistence `snapshot` plugin is not implemented yet.


## Setup

```scala
resolvers += Resolver.bintrayRepo("evolutiongaming", "maven")

libraryDependencies += "com.evolutiongaming" %% "kafka-journal" % "0.0.71"

libraryDependencies += "com.evolutiongaming" %% "kafka-journal-persistence" % "0.0.71"

libraryDependencies += "com.evolutiongaming" %% "kafka-journal-replicator" % "0.0.71"

libraryDependencies += "com.evolutiongaming" %% "kafka-journal-eventual-cassandra" % "0.0.71"

libraryDependencies += "com.evolutiongaming" %% "kafka-journal-prometheus" % "0.0.71"

libraryDependencies += "com.evolutiongaming" %% "kafka-journal-replicator-prometheus" % "0.0.71"
```


## Presentations

* Jan 2019 [Riga Scala Community](https://www.meetup.com/Riga-Scala-Community/events/257926307)
* Apr 2019 [Amsterdam.scala](https://www.meetup.com/amsterdam-scala/events/260005066/)
