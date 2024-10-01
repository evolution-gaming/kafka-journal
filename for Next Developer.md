Kafka Journal
=============

Currently, there is only one Cassandra specific implementation for replication storage. The storage is multi-tenant, 
while each instance of [Replicator](replicator/src/main/scala/com/evolutiongaming/kafka/journal/replicator/Replicator.scala) 
can serve only one Cassandra keyspace.

Currently following Cassandra tables are used in `replicator`'s keyspace:
* `metajournal` contains information about the "head" of "journals" (per `key` on `topic`)
* `journal` contains all appended events per "journal" (events for `key` on `topic`)
* `pointer2` - exposes per topic-partition offsets of last fully replicated message's offset on Kafka
* (technical) `setting` used to track scheme upgrades
* (technical) `locks` used to guarantee single DDL application during scheme upgrades 

# Most important aspects of "journals".

There are several things that describe "journals":
* [Key](core/src/main/scala/com/evolutiongaming/kafka/journal/Key.scala) (`id` + `topic`) mostly used as entity ID 
* [SeqNr](core/src/main/scala/com/evolutiongaming/kafka/journal/SeqNr.scala) (**1-based numbering**!) used by `akka-persistence` to track changes
  in actor's state since its creations (starts at `1`)
* [DeleteTo](core/src/main/scala/com/evolutiongaming/kafka/journal/DeleteTo.scala) used to track useful part of entity's
  journal:
  * part of entity's old journal can be removed till `seqNr` where same entity "restarts" its life, like reaches 
    "zero-state"
  * if snapshot of entity is made at `seqNr: 13`, then user can save `deleteTo: 13` to remove part of events, which are 
    not important anymore
* [PayloadMetadata](core/src/main/scala/com/evolutiongaming/kafka/journal/PayloadMetadata.scala) contains business value
* [ExpireAfter](core/src/main/scala/com/evolutiongaming/kafka/journal/ExpireAfter.scala) provides indication after how
  long entity can be fully removed (actual deletion happens in [PurgeExpired](replicator/src/main/scala/com/evolutiongaming/kafka/journal/replicator/PurgeExpired.scala))
* [SegmentNr](eventual-cassandra/src/main/scala/com/evolutiongaming/kafka/journal/eventual/cassandra/SegmentNr.scala)
  (**zero-based numbering**!) and [SegmentSize](eventual-cassandra/src/main/scala/com/evolutiongaming/kafka/journal/eventual/cassandra/SegmentSize.scala) 
  provides means to "split" journals in different Cassandra SSTables to more evenly distribute logical data on physical
  storage (don't ask why same name is used for 2 tables with different derivation algorithms):
  * in `metajournal` table `segment` is calculated from entity's `id`:
    ```scala
    val hashCode  = key.id.toLowerCase.hashCode
    def apply(hashCode: Int, segments: Segments): SegmentNr = {
      val segmentNr = math.abs(hashCode.toLong % segments.value)
      new SegmentNr(segmentNr) {}
    }
    ```
  * in `journal` table, it is calculated from entity's `seqNr` and `segmentSize`:
    ```scala
    def apply(seqNr: SeqNr, segmentSize: SegmentSize): SegmentNr = {
      val segmentNr = (seqNr.value - SeqNr.min.value) / segmentSize.value
      new SegmentNr(segmentNr) {}
    }
    ```

# Replicator

Main implementation of `replicator` happens in [ReplicatedCassandra](eventual-cassandra/src/main/scala/com/evolutiongaming/kafka/journal/eventual/cassandra/ReplicatedCassandra.scala).
There is full implementation of data adaption-replication from Kafka stream-like data to Cassandra-specific storage.

The [Replicator](replicator/src/main/scala/com/evolutiongaming/kafka/journal/replicator/Replicator.scala) initializes
Kafka consumers and Cassandra session to start [ReplicatedJournal](journal/src/main/scala/com/evolutiongaming/kafka/journal/eventual/ReplicatedJournal.scala).

[ReplicatedJournal](journal/src/main/scala/com/evolutiongaming/kafka/journal/eventual/ReplicatedJournal.scala):
* can expose list of so far discovered `topics`
* provides per-topic abstraction [ReplicatedTopicJournal](journal/src/main/scala/com/evolutiongaming/kafka/journal/eventual/ReplicatedTopicJournal.scala)

[ReplicatedTopicJournal](journal/src/main/scala/com/evolutiongaming/kafka/journal/eventual/ReplicatedTopicJournal.scala):
* provides API for per-topic-partition access [ReplicatedPartitionJournal](journal/src/main/scala/com/evolutiongaming/kafka/journal/eventual/ReplicatedTopicJournal.scala)
* is implemented in [ReplicatedCassandra](eventual-cassandra/src/main/scala/com/evolutiongaming/kafka/journal/eventual/cassandra/ReplicatedCassandra.scala)

[ReplicatedPartitionJournal](journal/src/main/scala/com/evolutiongaming/kafka/journal/eventual/ReplicatedTopicJournal.scala):
* provides API for `offsets` to expose and set how far topic-partition has been fully replicated
* provides access [ReplicatedKeyJournal](journal/src/main/scala/com/evolutiongaming/kafka/journal/eventual/ReplicatedKeyJournal.scala)
* is implemented in [ReplicatedCassandra](eventual-cassandra/src/main/scala/com/evolutiongaming/kafka/journal/eventual/cassandra/ReplicatedCassandra.scala)

[ReplicatedKeyJournal](journal/src/main/scala/com/evolutiongaming/kafka/journal/eventual/ReplicatedKeyJournal.scala) 
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
* is implemented in [ReplicatedCassandra](eventual-cassandra/src/main/scala/com/evolutiongaming/kafka/journal/eventual/cassandra/ReplicatedCassandra.scala)

## Aggregate's structure and its physical storage

Assuming that we have aggregate: `A`, with few journal events:`E0`, `E1`. It can:
* occupy both `metajournal` and `journal` tables
* occupy only `metajournal` table
* not exist in any table

Any aggregate has several important parts:
* `seqNr` - practically its age
* `deleteTo` - its meaningful "start" age, usually `seqNr` of aggregate's last "zero" state or snapshot
* `seqNr - deleteTo` journal records - ideally means that there is "exactly" so many events, 
  but there can be "gaps" (TODO MR: why? how?) 
* [Cassandra implementation specific detail] `segmentSize` - defines how entity is distributed on physical storage in 
  Cassandra

Generally sane, but wrong assumptions:
* aggregate's `deleteTo` is either `none` or has value that is no larger than `seqNr`
* `seqNr` is sequentially growing and doesn't "skip" values
* all aggregates use same `segmentSize`

### Cases on how records can be stored

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
  * when there is no entry in DB it will do nothing (no-op)
  * when there is entry for aggregate:
    * update `deleteTo` in `metajournal` table
    * delete events between aggregate's `deleteTo` and  `seqNr <= Delete.to` 
* `Purge` action:
  * when there is no entry in DB it will do nothing (no-op)
  * when there is entry for aggregate:
    * delete all events for aggregate from `journal` table between aggregate's `deleteTo` and `seqNr` including
    * delete entry in `metajournal` table








