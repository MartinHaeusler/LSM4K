# [C0001] Whole Commits

## What?

ChronoStore only ever puts "whole commits" into in-memory trees, and
always flushes "whole commits" to the persistent SST files.

This concept consists of two parts.

 - When updating in-memory stores, we always write the
**entire** change set of the transaction **atomically**
to the in-memory store. It **cannot** happen that a flush
task is started for a tree and only finds a partial
amount of changes to that store performed by a transaction.
The flush task will either find **all** changes or **none**.
 - The flush task **must** flush **all** in-memory changes
of a tree (which are visible to it) into the SST file. It
**must not** happen that a flush task picks a subset of
changes and flushes it to disk.

## Why?

Violating the "Whole Commits" principle would mean that
an SST file could exist which says "I contain the 
transaction with timestamp 1234", but it actually only
contains **a subset** of the changes performed by that
transaction to that store. This is highly problematic
for database recovery, because the question arises: 

> Do we need to replay the transaction with timestamp 1234
on this store or not?

If there can be partial data in the SST files, the answer to
this question **always needs to be YES**, because there
may always be data we've only partially written. This would
result in a lot of useless (but performance-intense) replays.

## How?

The WAL transaction groups changes by store (a `Map<StoreId, List<Command>>`).
It then proceeds to write the entire `List<Command>` to each individual
in-memory store. Each store accepts the list, and updates its in-memory
tree, which is an immutable data structure. Since we only ever update
the reference to the in-memory tree when the new in-memory tree has been
created, any flush task will see either **all** of the changes or **none**.

The flush-task itself is forced to take the entire list of commands from
the in-memory tree. For the unlikely event that there are two flush tasks
on the same tree at the same time, nothing bad will happen, as we simply
write the same data into two different SST files. The readers will perform
the deduplication, and once we perform a compaction, the duplicates will
be gone.

## Consequence

The consequence of this concept is the inherent system property that we
know for each SST file:

- which transaction was the last one that was stored in the file
- we can be certain that the transaction was stored fully
- we can be certain that no newer data is contained in the file