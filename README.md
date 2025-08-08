# Kafka Journal 
[![Build Status](https://github.com/evolution-gaming/kafka-journal/actions/workflows/ci.yml/badge.svg?branch=master)](https://github.com/evolution-gaming/kafka-journal/actions?query=workflow%3ACI+branch%3Amaster)
[![Coverage Status](https://coveralls.io/repos/github/evolution-gaming/kafka-journal/badge.svg?branch=master)](https://coveralls.io/github/evolution-gaming/kafka-journal?branch=master)
[![Codacy Badge](https://app.codacy.com/project/badge/Grade/a391e347e329454e8f992717113ec1ec)](https://app.codacy.com/gh/evolution-gaming/kafka-journal/dashboard?utm_source=gh&utm_medium=referral&utm_content=&utm_campaign=Badge_grade)
[![Version](https://img.shields.io/badge/version-click-blue)](https://evolution.jfrog.io/artifactory/api/search/latestVersion?g=com.evolution&a=kafka-journal_2.13&repos=public)
[![Chat](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/evolution-gaming/kafka-journal)

> Stream data from two sources where one is eventually consistent and the other one loses its tail

This library provides ability to use [Kafka](https://kafka.apache.org) as storage for events.
Kafka is a perfect fit in case you want to have streaming capabilities for your events.
However, it also uses [Cassandra](http://cassandra.apache.org) to keep data access performance on acceptable level and 
overcome Kafka's `retention policy`. 
Cassandra is a default choice, but you may use any other storage which satisfies following interfaces:
* Read side, called within client library: [EventualJournal](journal/src/main/scala/com/evolutiongaming/kafka/journal/eventual/EventualJournal.scala) 
* Write side, called from replicator app: [ReplicatedJournal](journal/src/main/scala/com/evolutiongaming/kafka/journal/eventual/ReplicatedJournal.scala) 

## High level idea

### Writing events flow:
1. Journal client publishes events to Kafka
2. Replicator app stores events in Cassandra

### Reading events flow:
1. Client publishes special marker to Kafka, so we can make sure there are no more events to expect
2. Client reads events from Cassandra, however at this point we are not yet sure that all events are replicated from 
   Kafka to Cassandra
3. Client reads events from Kafka using offset of last event found in Cassandra
4. We consider recovery finished when marker is read from Kafka

## Notes
* Kafka topic may be used for many different entities
* We don't need to store all events in Kafka as long as they are in Cassandra
* We do not cover snapshots, yet
* Replicator is a separate application
* It is easy to replace Cassandra with some relational database

## State recovery performance

Performance of reading events depends on finding the closest offset to the marker as well on replication latency (time 
difference between the moment event has been written to Kafka and the moment when event becomes available from
Cassandra).

Same Kafka consumer may be shared for many simultaneous recoveries.

## Read & write capabilities:
* Client allowed to `read` + `write` Kafka and `read` Cassandra
* Replicator allowed to `read` Kafka and `read` + `write` Cassandra

Hence, we recommend configuring access rights accordingly.

## Api

See [Journals](journal/src/main/scala/com/evolutiongaming/kafka/journal/Journals.scala) and 
[Journal](journal/src/main/scala/com/evolutiongaming/kafka/journal/Journal.scala)

## Troubleshooting

### Kafka exceptions in logs

Kafka client tends to log some exceptions at `error` level, however in reality those are harmless in case if operation
retried successfully. Retriable exceptions usually extend [RetriableException](https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/common/errors/RetriableException.java).

List of known "error" cases which may be ignored:
* Offset commit failed on partition .. at offset ..: The request timed out.
* Offset commit failed on partition .. at offset ..: The coordinator is loading and hence can't process requests.
* Offset commit failed on partition .. at offset ..: This is not the correct coordinator.
* Offset commit failed on partition .. at offset ..: This server does not host this topic-partition. 

## Akka persistence plugin

In order to use `kafka-journal` as [akka persistence plugin](https://doc.akka.io/libraries/akka-core/2.6/persistence-plugins.html) 
you have to add following to your `*.conf` file:
```hocon
akka.persistence.journal.plugin = "evolutiongaming.kafka-journal.persistence.journal"
```

Unfortunately akka persistence `snapshot` plugin is not implemented, yet.

## Pekko persistence plugin

In order to use `kafka-journal` as [pekko persistence plugin](https://pekko.apache.org/docs/pekko/1.0/persistence-plugins.html) 
you have to add following to your `*.conf` file:
```hocon
pekko.persistence.journal.plugin = "evolutiongaming.kafka-journal.persistence.journal"
```

Unfortunately pekko persistence `snapshot` plugin is not implemented, yet.

## Setup

```scala
addSbtPlugin("com.evolution" % "sbt-artifactory-plugin" % "0.0.2")

libraryDependencies += "com.evolution" %% "kafka-journal" % "5.0.0"

// for akka persistence
//libraryDependencies += "com.evolution" %% "kafka-journal-akka-persistence" % "5.0.0"

// for pekko persistence
libraryDependencies += "com.evolution" %% "kafka-journal-pekko-persistence" % "5.0.0"

libraryDependencies += "com.evolution" %% "kafka-journal-replicator" % "5.0.0"

libraryDependencies += "com.evolution" %% "kafka-journal-eventual-cassandra" % "5.0.0"
```

## Migration guide from 4.3.1 to 5.0.0

In `5.0.0` we adapted Evolution's new organization name and changed packages.
To migrate code from 4.3.1 to 5.0.0:
* change organization in `libraryDependencies` from `com.evolutiongaming` to `com.evolution`
* if used, update artefact names from `kafka-journal-persistence*` to `kafka-journal-akka-persistence*`
* change imports from `import com.evolutiongaming.kafka.journal` to `import com.evolution.kafka.journal`
* tech-related extensions were moved from `core` to corresponding tech modules, thus extra imports are required, like:
  * `com.evolution.kafka.journal.akkaext.OriginExtension` or `com.evolution.kafka.journal.pekko.OriginExtension` 
  * all Cassandra `encode*` and `decode*` extensions were moved to `kafka-journal-cassandra` module

## Presentations

* Jan 2019 [Riga Scala Community](https://www.meetup.com/Riga-Scala-Community/events/257926307)
* Apr 2019 [Amsterdam.scala](https://www.meetup.com/amsterdam-scala/events/260005066/)

## Development

To run unit-test, have to have Docker environment **running** (Docker Desktop, Rancher Desktop etc.). Some tests expect
to have `/var/run/docker.sock` available. In case of Rancher Desktop, one might need to amend local setup with:
```shell
sudo ln -s $HOME/.rd/docker.sock /var/run/docker.sock
```
