# Kafka Journal 
[![Build Status](https://github.com/evolution-gaming/kafka-journal/workflows/CI/badge.svg)](https://github.com/evolution-gaming/kafka-journal/actions?query=workflow%3ACI)
[![Coverage Status](https://coveralls.io/repos/github/evolution-gaming/kafka-journal/badge.svg?branch=master)](https://coveralls.io/github/evolution-gaming/kafka-journal?branch=master)
[![Codacy Badge](https://app.codacy.com/project/badge/Grade/a391e347e329454e8f992717113ec1ec)](https://app.codacy.com/gh/evolution-gaming/kafka-journal/dashboard?utm_source=gh&utm_medium=referral&utm_content=&utm_campaign=Badge_grade)
[![Version](https://img.shields.io/badge/version-click-blue)](https://evolution.jfrog.io/artifactory/api/search/latestVersion?g=com.evolutiongaming&a=kafka-journal_2.13&repos=public)
[![Chat](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/evolution-gaming/kafka-journal)

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

Hence, we recommend configuring access rights accordingly.

## Api

```scala
trait Journals[F[_]] {

  def apply(key: Key): Journal[F]
}

trait Journal[F[_]] {

  /**
   * @param expireAfter Define expireAfter in order to expire whole journal for given entity
   */
  def append(
    events: Nel[Event],
    expireAfter: Option[ExpireAfter],
    metadata: Option[JsValue],
    headers: Headers
  ): F[PartitionOffset]

  def read(from: SeqNr): Stream[F, EventRecord]

  def pointer: F[Option[SeqNr]]

  /**
   * Deletes events up to provided SeqNr, consecutive pointer call will return last seen value  
   */
  def delete(to: DeleteTo): F[Option[PartitionOffset]]

  /**
   * Deletes all data with regards to journal, consecutive pointer call will return none
   */
  def purge: F[Option[PartitionOffset]]
}
```

## Troubleshooting

### Kafka exceptions in logs

Kafka client tends to log some exceptions at `error` level, however in reality those are harmless in case of operation retried successfully.
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
addSbtPlugin("com.evolution" % "sbt-artifactory-plugin" % "0.0.2")

libraryDependencies += "com.evolutiongaming" %% "kafka-journal" % "0.0.153"

libraryDependencies += "com.evolutiongaming" %% "kafka-journal-persistence" % "0.0.153"

libraryDependencies += "com.evolutiongaming" %% "kafka-journal-replicator" % "0.0.153"

libraryDependencies += "com.evolutiongaming" %% "kafka-journal-eventual-cassandra" % "0.0.153"
```


## Presentations

* Jan 2019 [Riga Scala Community](https://www.meetup.com/Riga-Scala-Community/events/257926307)
* Apr 2019 [Amsterdam.scala](https://www.meetup.com/amsterdam-scala/events/260005066/)
