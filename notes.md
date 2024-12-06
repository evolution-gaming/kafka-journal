Kafka Journal
=============

# Requirements:
* Journal should be operational even when replication is offline, ideally up to topic retention period. Let's say 24h
* Journal will operate on huge streams of events, it is impossible to fit all in memory

# The challenge
Stream data from two storage when one is not consistent and the second lost his tail.

You might naively think that `kafka-journal` stores events in Kafka. That is not really true. It stores actions.

# Actions

Actions:
* **Append**: Kafka record that contains list of events saved atomically
* **Mark**: With help of `Mark` action we can prevent from consuming Kafka infinitely and stop upon marker found in topic
* **Delete**: Indicates attempt to delete all existing events up to passed `seqNr`. It will not delete future events

# Reading flow

1. `Action.Mark` pushed to the topic in parallel with querying Cassandra for processed topic offsets
2. Initiation query to Cassandra with streaming capabilities in parallel with initiating consumer from offsets just to 
   make sure there are no deletions
3. Start streaming data from Cassandra filtering out deleted records
4. When finished reading from Cassandra might need to start consuming data from Kafka, in case replication is slow or 
   does not happen at the moment

# Performance optimisations

1. If `Action.Mark`'s offset <= topic offsets from eventual storage, we don't need to consume Kafka at all (impossible best case 
   scenario, hope dies last)
2. No need to wait for `Action.Mark` produce completed to start querying Cassandra
3. In case reading Kafka while looking up for `Action.Delete`, we can buffer some part of head to not initiate second 
   read (most common scenario, replicating app is on track)
4. We don't need to initiate second consumer in case we managed to replicate all within reading duration
5. Use pool of consumers and cache either head or deletions
6. Subscribe for particular partition rather than the whole topic, however this limits caching capabilities
7. Don't deserialise unrelated Kafka records

# Corner cases

* We cannot stream data to client before making sure there are no `Action.Delete` in not yet replicated Kafka head

# TODO

* Measure three important operations of Kafka consumer: init, seek, poll. So we can optimise journal even better
* More corner cases to come in order to support re-partition [＾_＾]
* Decide on when to clean/cut head caches
