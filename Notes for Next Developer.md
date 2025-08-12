# Kafka Journal

> [!WARNING] 
> Very possibly the information in this file is outdated!

## Requirements

* Journal should be operational even when replication is offline, ideally up to topic retention period. Let's say 24h
* The Journal will operate on huge streams of events, it is impossible to fit all in memory

## The challenge

Stream data from two storage when one is not consistent and the second lost his tail.

You might naively think that `kafka-journal` stores events in Kafka. That is not really true. It stores actions.

## Actions

Actions:
* **Append**: Kafka record that contains list of events saved atomically
* **Mark**: With help of `Mark` action we can prevent from consuming Kafka infinitely and stop upon marker found in topic
* **Delete**: Indicates attempt to delete all existing events up to passed `seqNr`. It will not delete future events
* **Purge**: Indicates attempt to delete the whole journal fully - leave no data behind. The next **Append** will create a new journal

## Reading flow

1. `Action.Mark` is pushed to the topic in parallel with querying Cassandra for processed topic offsets
2. Concurrently:
  * initiated query to Cassandra with streaming capabilities
  * initiated Kafka consumer from last known offsets just to make sure there are no deletions
3. Start streaming data from Cassandra filtering out deleted records
4. When finished reading from Cassandra, kafka-journal might need to start consuming data from Kafka, in case replication is slow
   or does not happen at the moment

## Performance optimizations

1. If `Action.Mark`'s offset <= topic's offset from eventual storage, we don't need to consume Kafka at all (impossible best case
   scenario)
2. Don't wait for `Action.Mark` being fully published before start of Cassandra querying
3. In case reading Kafka while looking up for `Action.Delete`, we can buffer some part of head to not initiate second
   read (most common scenario, replicating app is on track)
4. We don't need to initiate second consumer in case we managed to replicate all events within reading duration
5. Use the pool of consumers and cache either head or deletions
6. Subscribe for a particular partition rather than the whole topic, however, this limits caching capabilities
7. Don't deserialize unrelated Kafka records

## Corner cases

* We cannot stream data to client before making sure there are no `Action.Delete` in not yet replicated Kafka head

## TODO

* Measure three important operations of Kafka consumer: init, seek, poll. So we can optimise journal even better
* More corner cases to come in order to support re-partition [＾_＾]
* Decide on when to clean/cut head caches

# Implementation details

Currently, there is only one Cassandra specific implementation for replication storage. The storage is multi-tenant, 
while each instance of [Replicator](replicator/src/main/scala/com/evolution/kafka/journal/replicator/Replicator.scala) 
can serve only one Cassandra keyspace.

Currently following Cassandra tables are used in `replicator`'s keyspace:
* `metajournal` contains information about the "head" of "journals" (per `key` on `topic`)
* `journal` contains all appended events per "journal" (events for `key` on `topic`)
* `pointer2` - exposes per topic-partition offsets of last fully replicated message's offset on Kafka
* (technical) `setting` used to track scheme upgrades
* (technical) `locks` used to guarantee single DDL application during scheme upgrades 

## Most important aspects of "journals".

There are several things that describe "journals":
* [Key](core/src/main/scala/com/evolution/kafka/journal/Key.scala) (`id` + `topic`) mostly used as entity ID 
* [SeqNr](core/src/main/scala/com/evolution/kafka/journal/SeqNr.scala) (**1-based numbering**!) used by `akka-persistence` or `pekko-persistence` to track changes
  in actor's state since its creations (starts at `1`)
* [DeleteTo](core/src/main/scala/com/evolution/kafka/journal/DeleteTo.scala) used to track useful part of entity's journal:
  * part of entity's old journal can be removed till `seqNr` where same entity "restarts" its life, like reaches 
    "zero-state"
  * if snapshot of entity is made at `seqNr: 13`, then user can save `deleteTo: 13` to remove part of events, which are 
    not important any more
* [PayloadMetadata](core/src/main/scala/com/evolution/kafka/journal/PayloadMetadata.scala) contains business value
* [ExpireAfter](core/src/main/scala/com/evolution/kafka/journal/ExpireAfter.scala) provides an indication after how
  long entity can be fully removed (actual deletion happens in [PurgeExpired](replicator/src/main/scala/com/evolution/kafka/journal/replicator/PurgeExpired.scala))
* [SegmentNr](eventual-cassandra/src/main/scala/com/evolution/kafka/journal/eventual/cassandra/SegmentNr.scala)
  (**zero-based numbering**!) and [SegmentSize](eventual-cassandra/src/main/scala/com/evolution/kafka/journal/eventual/cassandra/SegmentSize.scala) 
  provides means to "split" journals in different Cassandra SSTables to more evenly distribute logical data on physical
  storage (don't ask why the same name is used for 2 tables with different derivation algorithms):
  * in `metajournal` table `segment` is calculated from entity's `id`:
    ```scala
    val hashCode  = key.id.toLowerCase.hashCode
    def apply(hashCode: Int, segments: Segments): SegmentNr = {
      val segmentNr = math.abs(hashCode.toLong % segments.value)
      new SegmentNr(segmentNr) {}
    }
    ```
    See: [SegmentNr.metaJournal](eventual-cassandra/src/main/scala/com/evolution/kafka/journal/eventual/cassandra/SegmentNr.scala)
  * in `journal` table, it is calculated from entity's `seqNr` and `segmentSize`:
    ```scala
    def apply(seqNr: SeqNr, segmentSize: SegmentSize): SegmentNr = {
      val segmentNr = (seqNr.value - SeqNr.min.value) / segmentSize.value
      new SegmentNr(segmentNr) {}
    }
    ```
    See: [SegmentNr.journal](eventual-cassandra/src/main/scala/com/evolution/kafka/journal/eventual/cassandra/SegmentNr.scala)

## Replicator

Main implementation of `replicator` happens in [ReplicatedCassandra](eventual-cassandra/src/main/scala/com/evolution/kafka/journal/eventual/cassandra/ReplicatedCassandra.scala).
There is full implementation of data adaption-replication from Kafka stream-like data to Cassandra-specific storage.

The [Replicator](replicator/src/main/scala/com/evolution/kafka/journal/replicator/Replicator.scala) initializes
Kafka consumers and Cassandra session to start [ReplicatedJournal](journal/src/main/scala/com/evolution/kafka/journal/eventual/ReplicatedJournal.scala).

[ReplicatedJournal](journal/src/main/scala/com/evolution/kafka/journal/eventual/ReplicatedJournal.scala):
* can expose list of so far discovered `topics`
* provides per-topic abstraction [ReplicatedTopicJournal](journal/src/main/scala/com/evolution/kafka/journal/eventual/ReplicatedTopicJournal.scala)

[ReplicatedTopicJournal](journal/src/main/scala/com/evolution/kafka/journal/eventual/ReplicatedTopicJournal.scala):
* provides API for per-topic-partition access [ReplicatedPartitionJournal](journal/src/main/scala/com/evolution/kafka/journal/eventual/ReplicatedTopicJournal.scala)
* is implemented in [ReplicatedCassandra](eventual-cassandra/src/main/scala/com/evolution/kafka/journal/eventual/cassandra/ReplicatedCassandra.scala)

[ReplicatedPartitionJournal](journal/src/main/scala/com/evolution/kafka/journal/eventual/ReplicatedTopicJournal.scala):
* provides API for `offsets` to expose and set how far topic-partition has been fully replicated
* provides access [ReplicatedKeyJournal](journal/src/main/scala/com/evolution/kafka/journal/eventual/ReplicatedKeyJournal.scala)
* is implemented in [ReplicatedCassandra](eventual-cassandra/src/main/scala/com/evolution/kafka/journal/eventual/cassandra/ReplicatedCassandra.scala)

[ReplicatedKeyJournal](journal/src/main/scala/com/evolution/kafka/journal/eventual/ReplicatedKeyJournal.scala) 
defines the **most important API** of the `replicator` - it handles all actions and applies them on Cassandra storage.
The APIs are per `key` (`id` + `topic`):
* `append` - appends journal entries into `journal` table and updates `seqNr`, `offset` and `updated` fields in
  `metajournal` table 
* `delete` - deletes entries from `journal` table till requested `seqNr` and updates `deleteTo`, `offset` and `updated` 
  fields in `metajournal` table, on next recovery entity will:
  * if `deleteTo` is less than `seqNr`, get recovered from left-over events
  * if `delerteTo` is equal to `seqNr`, recovered actor will start with zero-state at `seqNr`
* `purge` - delete all journal's entries in `journal` table as well as removed entry in `metajournal` table - next
  reincarnation of journal will start from very beginning with `seqNr: 1`
* is implemented in [ReplicatedCassandra](eventual-cassandra/src/main/scala/com/evolution/kafka/journal/eventual/cassandra/ReplicatedCassandra.scala)

## Aggregate's structure and its physical storage

Assuming that we have aggregate: `A`, with few journal events:`E0`, `E1`. It can:
* occupy both `metajournal` and `journal` tables
* occupy only `metajournal` table
* not exist in any table

Any aggregate has several important parts:
* `seqNr` - practically its age
* `deleteTo` - its meaningful "start" age, usually `seqNr` of aggregate's last "zero" state or snapshot
* `seqNr - deleteTo` journal records - ideally means that there are "exactly" so many events, 
  but there can be "gaps" (for example in `append` action larger value for `seqNr` is set) 
* [Cassandra implementation specific detail] `segmentSize` - defines how entity is distributed on physical storage in 
  Cassandra

Generally correct, but wrong assumptions:
* aggregate's `deleteTo` is either `none` or has value that is no larger than `seqNr`
* `seqNr` is sequentially growing and doesn't "skip" values
* all aggregates use same `segmentSize`

## Cases on how records can be stored

| aggregate in `metajournal` table              | `journal` table `(seqNr, segmentNr)`                | `snapshot` table `(seqNr)` | notes                                     |
|-----------------------------------------------|-----------------------------------------------------|----------------------------|-------------------------------------------|
| `A(seqNr: 3, deleteTo: none, segmentSize: 5)` | `E(1,0)`, `E(2, 0)`, `E(3, 0)`                      |                            |                                           |
| `A(seqNr: 5, deleteTo: none, segmentSize: 3)` | `E(1,0)`, `E(2, 0)`, `E(3, 0)`, `E(4,1)`, `E(5, 1)` |                            |                                           |
| `A(seqNr: 5, deleteTo: 2, segmentSize: 3)`    | `E(3, 0)`, `E(4,1)`, `E(5, 1)`                      |                            | events are stored in 2 segments           |
| `A(seqNr: 5, deleteTo: none, segmentSize: 3)` | `E(1,0)`, `E(2, 0)`, `E(3, 0)`, `E(4,1)`, `E(5, 1)` | `S(5)`                     | all events are preserved and has snapshot |
| `A(seqNr: 5, deleteTo: 5, segmentSize: 3)`    |                                                     |                            | all events are deleted, "zero state"      |
| `A(seqNr: 5, deleteTo: 5, segmentSize: 3)`    |                                                     | `S(5)`                     | all events are deleted, has snapshot      |
| `A(seqNr: 5, deleteTo: 5, segmentSize: 3)`    |                                                     | `S(2)`, `S(5)`             | all events are deleted, has 2 snapshots   |

## Expected action behaviour

* `Mark` action is no-op for changes in storage
* `Append` action:
  * when there is no entry in DB:
    * create record in `metajournal` table with `seqNr` of last event and `deleteTo: none`
    * append events in `journal` table
  * when there is entry for aggregate:
    * update `seqNr` in `metajournal` table
    * append events in `journal` table
* `Delete` action:
  * when there is no entry in DB, it will:
    * create record in `metajournal` table with `seqNr` and `deleteTo` both set to `Delete.to` value (allows to "reset" journal)
  * when there is entry for aggregate:
    * update `deleteTo` in `metajournal` table
    * delete events between aggregate's `deleteTo` and  `seqNr <= Delete.to` 
* `Purge` action:
  * when there is no entry in DB, it will do nothing (no-op)
  * when there is entry for aggregate:
    * delete all events for aggregate from `journal` table between aggregate's `deleteTo` and `seqNr` including
    * delete entry in `metajournal` table
* `Append` and `Delete` actions also update `partition` and `offset` fields in `metajournal` table to allow easier 
  lookup of action, which caused the last change, in Kafka
