# LSM4K

A Transactional LSM-based Key-Value Storage Engine for Kotlin (and other JVM
languages).

## Introduction

LSM4k is a high-performance Key-Value store for the Java Virtual Machine.
It is written in Kotlin
with a Kotlin-first API in mind. Other JVM languages (such as Java, Scala,
Groovy, etc.) may leverage it
as well, but expect some friction in the API.


## Including in your project

In gradle:

```kotlin
implementation("io.github.martinhaeusler.lsm4k:LSM4K:1.0.0-Alpha")
// include the snappy compressor (not required, but recommended)
implementation("io.github.martinhaeusler.lsm4k:compressor-snappy:1.0.0-Alpha")
```

In maven:

```xml
<dependency>
  <groupId>io.github.martinhaeusler.lsm4k</groupId>
  <artifactId>LSM4K</artifactId>
  <version>1.0.0-Alpha</version>
</dependency>

<!-- Snappy Compressor (not required, but recommended) -->
<dependency>
    <groupId>io.github.martinhaeusler.lsm4k</groupId>
    <artifactId>compressor-snappy</artifactId>
    <version>1.0.0-Alpha</version>
</dependency>
```

## Project State

This project is currently in **1.0.0-Alpha** state and requesting feedback from developers and users.

> **IMPORTANT:** I reserve the right to update the API as well as the persistent format without
> prior notice or migration path until release version 1.0.0 (stable) is reached.

All constructive feedback is welcome.

- Found a problem / inconsistency / other wrinkle with the API? Let me know.
- Found a problem in the persistent format? Let me know.
- Found a bug? Let me know (a reproducing minimal JUnit test would be very much appreciated)
- Found a performance bottleneck? Let me know (a minimal benchmark which demonstrates the issue would be appreciated)

## Basic Usage

```kotlin
// open a new DatabaseEngine instance on a directory of your choice
DatabaseEngine.openOnDirectory(directory).use { engine ->
    // option 1 to open transactions: using the structured lambda 
    // (this is the preferred way) 
    engine.readWriteTransaction { tx ->
        // create a new named store
        tx.createNewStore("example")
        tx.commit()
    }

    // option 2 to open transactions: using beginTransaction()
    // (Java users can use try-with-resources instead of ".use()")
    engine.beginReadWriteTransaction().use { tx ->
        val store = tx.getStore("example")
        // insert or update using "put(key, value)"
        store.put(Bytes.of("hello"), Bytes.of("world"))
        store.put(Bytes.of("foo"), Bytes.of("bar"))

        // you can get individual values for keys. The values
        // can be transient in your current transaction, or
        // persistent on disk.
        store.get(Bytes.of("foo"))?.asString() // gives "bar"
        store.get(Bytes.of("notThere"))?.asString() // gives null

        // you can iterate over the data in the store via cursors
        // (Java users: please use 'useCursor(...)' instead)
        store.withCursor { cursor ->
            // navigate using:
            // - first / last
            // - next / prev
            // - seekExactlyOrNext / seekExactlyOrPrev
            cursor.firstOrThrow()
            // access the current position
            val firstKey = cursor.key.asString() // gives "foo"
            val firstValue = cursor.value.asString() // gives "bar"

            // safe navigation is supported as well: the navigation
            // methods return true if the navigation was successful,
            // or false if they could not be executed because there
            // is no more data.
            if (!cursor.next()) {
                // no more enries
                return@withCursor
            }

            val secondKey = cursor.key.asString() // gives "hello"
            val secondValue = cursor.value.asString() // gives "world"
        }

        tx.commit()
    }

} // instance gets closed here automatically
```

## Configuration

LSM4K comes with a sensible default configuration which allows for minimal dependencies
and should get you started. You can configure and fine-tune various aspects. Kotlin users
can either use the builder pattern (see below) or instantiate the configuration with
named arguments directly:

```kotlin
val config = LSM4KConfiguration(
    blockCacheSize = 4.GiB,
    // ... other settings by name ...
)
```

Java users don't have the luxury of named arguments. You can use the provided builder pattern
instead (which also works for Kotlin if you prefer this solution):

```java
var config = LSM4KConfiguration.builder()
    .withBlockCacheSize(4, SizeUnit.GIB)
    // ... other settings
    .build();
```

For the full list of settings and their defaults, please refer to your IDE code completion,
the JavaDoc or the source code directly.

Once you have established a configuration, you can pass it into the factory method as
the second argument.

In Kotlin:

```kotlin
// create config as shown above
val config = LSM4KConfiguration()

DatabaseEngine.openOnDirectory(directory, config).use { engine ->
    // use the engine
}
```

In Java:

```java
// create config as shown above
LSM4KConfiguration config = LSM4KConfiguration.builder().build();

try(DatabaseEngine engine = DatabaseEngine.openOnDirectory(directory, config)){
    // use the engine
}
```

## Features

### Transactional API

Transactions are at the heart of LSM4K. All transactions are ACID:

- **Atomic**: All changes in a transaction are committed, or none of them.
- **Consistent**: Every transaction reads a consistent snapshot of the data.
- **Isolated**: Concurrent transactions do not interfere with each other, every
  transaction is virtually "alone".
- **Durable**: Once the transaction commit is completed, the data is guaranteed
  to be durable on disk.

They adhere to **snapshot isolation**, which means that every transaction `T`
can only read the changes of transactions which have completed before `T` has started.
All reads performed by `T` are **repeatable**: no matter what other transactions are doing
concurrently, `T` can repeat any individual read or compound cursor operation as often as
it wants to, it will always receive the same result. The only exception to this rule is
when `T` performed transient changes to the data itself between the read operations,
then these changes will be reflected in the results.

LSM4K supports three types of transactions:

- Read-Only Transactions
- Read-Write Transactions
- Exclusive Transactions

#### Read-Only Transactions

Read-Only Transactions may only read data, but cannot commit any changes. They may,
however, apply temporary changes which are only visible to them. In other words, calling
`store.put(key, value)` is fine in a read-only transaction, only `commit()` is not allowed.
There may be multiple concurrent transactions of this type at any point in time.

The following operations are allowed / not allowed:

| Operation               | Example                                                     | Permitted |
|-------------------------|-------------------------------------------------------------|-----------|
| Read from Stores        | `store.get(...)`, `store.openCursor()`                      | ✅ Yes     |
| Transient modifications | `store.put(...)`, `store.delete()`                          | ✅ Yes     |
| Create stores           | `transaction.createNewStore(...)`, `store.deleteStore(...)` | ❌ No      |
| Commit changes          | `transaction.commit()`                                      | ❌ No      |

Attempting to perform an action which is not permitted will result in an exception.
While a transaction in this mode is active, the following concurrent transactions are allowed / not allowed:

| Concurrent Transaction | Permitted |
|------------------------|-----------|
| Read-Only              | ✅ Yes     |
| Read-Write             | ✅ Yes     |
| Exclusive              | ✅ Yes     |

Starting a Read-Only Transaction in Kotlin:

```kotlin
// using lambdas (recommmended):
engine.readOnlyTransaction { tx ->
    // ... perform changes & queries...

    // runtime exception, tx is read-only!
    // Do NOT attempt to commit a read-only transaction! 
    tx.commit()
}

// or explicitly:
engine.beginReadOnlyTransaction().use { tx ->
    // ... perform changes & queries...

    // runtime exception, tx is read-only!
    // Do NOT attempt to commit a read-only transaction! 
    tx.commit()
}
```

In Java:

```kotlin
// using lambdas (recommended):
engine.readOnlyTransaction((tx) -> {
    // ... perform changes & queries...

    // runtime exception, tx is read-only!
    // Do NOT attempt to commit a read-only transaction! 
    return tx.commit();
});

// or explicitly:
try (var tx = engine.beginReadOnlyTransaction()){
    // ... perform changes & queries...

    // runtime exception, tx is read-only!
    // Do NOT attempt to commit a read-only transaction! 
    return tx.commit();
}
```

#### Read-Write Transactions

Read-Write Transactions may read data and write data. In contrast to read-only transactions, they are allowed
(and expected) to perform a `commit()` at the end. There may be multiple concurrent
transactions of this type at any point in time. This type of transaction operates on the
Snapshot isolation level.

The following operations are allowed / not allowed:

| Operation               | Example                                                     | Permitted |
|-------------------------|-------------------------------------------------------------|-----------|
| Read from Stores        | `store.get(...)`, `store.openCursor()`                      | ✅ Yes     |
| Transient modifications | `store.put(...)`, `store.delete()`                          | ✅ Yes     |
| Create stores           | `transaction.createNewStore(...)`, `store.deleteStore(...)` | ✅ Yes     |
| Commit changes          | `transaction.commit()`                                      | ✅ Yes     |

Attempting to perform an action which is not permitted will result in an exception.
While a transaction in this mode is active, the following concurrent transactions are allowed / not allowed:

| Concurrent Transaction | Permitted |
|------------------------|-----------|
| Read-Only              | ✅ Yes     |
| Read-Write             | ✅ Yes     |
| Exclusive              | ✅ No      |

Starting a Read-Write Transaction in Kotlin:

```kotlin
// using lambdas (recommended):
engine.readWriteTransaction { tx ->
    // ... perform changes & queries
    tx.commit()
}

// or explicitly:
engine.beginReadWriteTransaction().use { tx ->
    // ... perform changes & queries
    tx.commit()
}
```

In Java:

```java
// using lambdas (recommended):
engine.readWriteTransaction((tx) ->{
    // ... perform changes & queries
    return tx.commit();
});

// or explicitly:
try(var tx = engine.beginReadWriteTransaction()){
    // ... perform changes & queries
    return tx.commit();
}
```

#### Exclusive Transactions

Exclusive Transactions have the same properties as Read-Write Transactions, but only one Exclusive Transaction
may be active at any point in time. While it is active, it blocks other Read-Write and
Exclusive Transactions from being executed. This transaction type should be used sparingly
and only if strict Serializable write semantics are required.

The following operations are allowed / not allowed:

| Operation               | Example                                                     | Permitted |
|-------------------------|-------------------------------------------------------------|-----------|
| Read from Stores        | `store.get(...)`, `store.openCursor()`                      | ✅ Yes     |
| Transient modifications | `store.put(...)`, `store.delete()`                          | ✅ Yes     |
| Create stores           | `transaction.createNewStore(...)`, `store.deleteStore(...)` | ✅ Yes     |
| Commit changes          | `transaction.commit()`                                      | ✅ Yes     |

Attempting to perform an action which is not permitted will result in an exception.

While a transaction in this mode is active, the following concurrent transactions are allowed / not allowed:

| Concurrent Transaction | Permitted |
|------------------------|-----------|
| Read-Only              | ✅ Yes     |
| Read-Write             | ✅ No      |
| Exclusive              | ✅ No      |

Starting an exclusive transaction in Kotlin:

```kotlin
// using lambdas (recommended):
engine.exclusiveTransaction { tx ->
    // ... perform changes & queries
    tx.commit()
}

// or explicitly:
engine.beginExclusiveTransaction().use { tx ->
    // ... perform changes & queries
    tx.commit()
}
```

Starting an exclusive transaction in Java:

```java
// using lambdas (recommended):
engine.exclusiveTransaction((tx) ->{
    // ... perform changes & queries
    return tx.commit();
});

// or explicitly:
try(var tx = engine.beginExclusiveTransaction()){
    // ... perform changes & queries
    tx.commit();
}
```

### Log-Structured-Merge (LSM) Tree

LSM4k follows the Log-Structured-Merge-Tree architecture instead of
classical B+ trees.
This allows LSM4K to...

- ... work in a strictly **append-only** fashion. Existing data is never
  overwritten, only new
  files are being created as needed. If a file becomes obsolete, it is dropped
  in its entirety.
  This allows for high data consistency with minimal locking.
- ... employ a high degree of **compression** on its data. This helps to keep
  the disk footprint
  small and speeds up reads considerably.
- ... trade **random reads and writes** traditionally required in B+ trees for *
  *sequential access**.
- ... respond quickly to API requests while deferring reorganizational work to
  background threads.

### Compression

Compression is another main pillar of LSM4K. All LSM files utilize
[Snappy](https://github.com/xerial/snappy-java) compression by default which
offers a good balance
between (de-)compression speed and file size. Employing compression allows for *
*faster** reads too:
the time spent for reading + decompression is **less** than the time for reading
the corresponding
uncompressed data from disk.

### Write-Ahead-Log

LSM4K utilizes a Write-Ahead-Log (WAL) to ensure atomic and durable
writes. The WAL format is
resilient against truncation and modification. Even in the event of a crash,
process kill or power
outage, LSM4K will be able to recover to a working and consistent state.
In case of actual
file corruption due to faulty drives or direct manipulation from outside,
LSM4K is able to
detect and report the corruption and exit gracefully.

To prevent the Write-Ahead-Log from growing indefinitely, LSM4K employs
periodic WAL shortening
which drops already-processed files that are no longer needed for recovery as
part of its Checkpoint
algorithm.

### Manifest

While the WAL deals with the key-value pairs themselves, the Manifest is
concerned with the internal
structure of the store. It keeps track of which stores exist, which transactions
can see them, and
how the files in each individual store fit into its organizational structure.
Similar to the WAL,
the manifest format is designed in a way that is resilient against truncation
and can detect corruption.

### Compaction

Compaction is the background process which allows the LSM tree to rebalance
itself. It needs to be executed
regularly in order to maintain adequate read performance. You can configure
a global compaction strategy which will be used by all new stores. This strategy
can then be overridden
on a per-store basis. Like many other LSM implementations, LSM4K offers
two compaction algorithms:
Tiered Compaction and Leveled Compaction.

#### Tiered Compaction

Tiered Compaction organizes the files in a store in "tiers": every tier contains
a full sorted run of the
data. As new data comes in, the number of tiers increases over time. Through
compaction, the maximum
number of tiers is reduced again.

#### Leveled Compaction

Leveled Compaction organizes the files in a store in "levels": every level
contains multiple files with
disjoint key ranges. Data moves up from one level to the next over time. The
maximum level is
pre-determined by the configuration and is never exceeded. The key idea here is
that older data (which
lives in higher levels) changes less frequently than newer data (which lives in
lower levels).

### Block Cache

LSM4K employs a sophisticated second-level cache for data blocks which is
shared among all
transactions for maximum read performance. You can control the desired size of
the cache via the
configuration.

### Designed for Larger-Than-RAM data

While LSM4K aims to make the best use of the available RAM allocated to it
and its caches,
it never assumes that all data in the store will fully fit into main memory. You
can very well
iterate over a store that contains 100 GiB of data with a JVM that has 1GiB of
heap space in total.

### Asynchronous Prefetching

When using a cursor to iterate over a store, the cursor tries to predict which
data block will be needed
next. When it determines which block it will likely need, it immediately
triggers a load task in the
background. By the time your application code is calling `next()` or
`previous()`, chances are that the
block has already finished loading and is available to the cursor immediately.
This greatly reduces
cursor stalling due to I/O operations. There is a fixed number of prefetching
threads which serve the
reqeusts of all transactions. If there are few concurrent transactions, multiple
prefetching threads
will optimize the read performance of a single transaction. When there are more
transactions than
prefetching threads, the workload from the transactions is distributed
accordingly. You can control the
number of prefetching threads in the configuration (or disable prefetching
altogether if desired) and it
should match the concurrent requests supported by your storage device.

### In-Memory Mode

LSM4K is built on top of a (very simple) Virtual File System. This allows
LSM4K to offer a
full in-memory mode without any changes in its public API. This mode is
particularly useful for automated
test suites, as they usually operate on smaller datasets, are executed
frequently, and not accessing the
file system means that no cleanup work needs to be done after a test has
completed its execution.

### Bloom Filters

Like many other LSM trees, LSM4K employs Bloom Filters, a probabilistic
data structure which can tell if a key **may** be contained in a file or
is **certainly not** contained. This allows LSM4K to skip over entire
files when searching for a key. A bloom filter is created for each LSM
file and is stored in its header.

**Important:** Bloom filters only help to improve the speed of `store.get(key)`
requests in particular. They do not benefit cursor-based operations.

### Minimal-Copy Approach

LSM4K employs a data management approach that minimizes copying. There are
exactly two places
where copies can occur:

- When a (part of a) file is read from the operating system, the memory is
  copied from the OS into the
  JVM heap.
- When data needs to be decompressed, a new buffer is allocated to hold the
  decompressed data.
  This decompression process is (in a way) a copy process.

Notably, **no** data copying takes place when data is served from the Block
Cache to a transaction.
LSM4K makes extensive use of the immutable `Bytes` interface which allows
to read data, but prevents modification. Furthermore, `Bytes` (unlike raw byte arrays)
internally provides zero-copy slicing, so all data eventually is a direct or indirect
reference to an element in the block cache.

There is one downside to this approach: if you want to retain access to a key or
value you have fetched for extended periods of time, you should call `bytyes.own()`
on it to create a local copy. Not doing this means that a reference to the block
in the block cache is retained in the JVM heap, which may lead to `OutOfMemoryError`s.
However, the usual use case for key-value stores is that each value will
be deserialized immediately on-the-fly into a format which is usable by the
application, so this problem should not occur often.

### Fully on-heap

In order to maximize ease of use, ease of configuration, ease of deployment and
ease of debugging, LSM4K operates fully on-heap. It only allocates temporary off-heap
buffers when it needs to communicate with the file system, or when it calls into
native APIs for compression. These buffers are short-lived and usually small in size.
All data which out-lives a transaction will be held on-heap. A heap dump will
therefore provide exact information to you. When choosing the maximum
heap size of your JVM, leaving headroom for off-heap operations is therefore
much less of a concern compared to an off-heap solution.

## Building

LSM4K is built with Gradle:

```shell
  ./gradlew build
```

The automated test suite can be executed indivdually via:

```shell
  ./gradlew test
```

All dependencies are managed via the version catalog located in
`/gradle/libs.versions.toml`.

## Contributing

Contributions are welcome! All structured bug reports, JUnit tests and feature
implementations
are welcome, provided that they meet the quality standards and fit into the
existing code base
and vision.

## FAQ (Frequently Asked Questions)

### Q: Can I use this library with Java / Scala / Groovy / ... ?

The API has been designed predominantly with Kotlin and its language features in mind.
That being said, I tried to make it accessible from Java as much as possible. It may
not be as nice as when using Kotlin directly, but it should work. By extension, if it
works for Java, it should also work for all other languages which offer interoperability
with Java libraries.

### Q: What's the big difference between LSM and a B-Tree?

A B-Tree is usually managed in data "pages". Each page has the same size, and
a page manager loads them from disk as needed and caches them. When a modification
on a B-Tree occurs, all the pages it touches are marked as "dirty". A dirty page
must not be discarded from the cache before it is written to disk. Additionally,
concurrent modifications on those pages need to be synchronized accordingly. There
are some major problems with this approach:

- A single modification (e.g. a single insert) may cause the B-Tree to rebalance itself
  via a so-called "[rotation](https://en.wikipedia.org/wiki/Tree_rotation)". These
  rotations touch **many** nodes and **many** pages of the tree. This consequently
  means **many** writes to disk, all of which are **random** (i.e. not sequential)
  since the pages live in arbitrary positions on disk. This increases the so-called
  [write amplification](https://en.wikipedia.org/wiki/Write_amplification), i.e. the
  number of actual writes required for a single logical write.
- The tree rotation algorithm itself is fairly involved and complex.
- Since the algorithm touches multiple pages, write atomicity becomes an issue. B-Trees
  can recover from partial writes with a Write-Ahead-Log (WAL) (which is also employed
  by LSM4K), but the **recovery algorithm** is extremely complex
  (see [ARIES](https://en.wikipedia.org/wiki/Algorithms_for_Recovery_and_Isolation_Exploiting_Semantics)).
- B-Tree implementations tend to apply writes in a **synchronous** manner: even though the
  data provided by the user is in the Write-Ahead-Log, they still use the request thread
  for updates in the tree itself to ensure that subsequent reads will have access to them.
- Since changes can occur on any page at any point in time, B-Trees do not lend themselves
  well to compression algorithms.

LSM trees are like a day-and-night difference to B-trees when it comes to these points:

- They operate on **immutable** data structures. This makes recovery quick and easy.
- They perform exclusively **sequential writes** by appending to existing files. Unlike
  the B-tree, there are no random writes on tree nodes, since everything is immutable.
- Since all files on disk are immutable, compression algorithms are a natural fit for
  LSM trees.
- Since LSM files on disk are immutable, only a bare minimum of locking is required.
  Individual blocks (unlike pages in B-Trees) neither require locking nor dirty-flagging
  as they will never change their content.
- Since LSM files on disk are immutable, the recovery algorithm is fairly straight forward:
  it deletes any files which haven't been fully written and keeps the rest intact. Some more
  effort is required for the Write-Ahead-Log recovery itself, which is the same procedure as
  for the B-Tree WAL. LSM trees **never allow uncommitted changes** to enter LSM files;
  rollbacks during recovery are therefore not needed.
- When a commit occurs, data is only written into the Write-Ahead-Log and into the in-memory
  segment of the affected LSM tree. The committing thread is released immediately afterward.
  This means that writes will be **very fast** since all they do is essentially to dump their
  changes into the WAL, which is the bare minimum amount of work needed to ensure atomicity
  and durability.
- If too many writes occur in quick succession, the in-memory segments of LSM trees may
  become too large, which means that the ongoing commit has to wait until the in-memory
  data has been flushed to disk. This does block the writer temporarily but should only
  occur in very write-heavy scenarios. Configuring the available memory pool for the
  in-memory segments can help to address this situation.
- LSM trees do not have a "rebalancing" mechanism as such, but they do need to reorganize
  their data from time to time. This is called a **compaction**. Compactions run entirely
  on background threads. While they do consume system resources (CPU cycles, RAM and
  disk I/O operations), they **do not block** other concurrent operations on the LSM tree.
  Neither writers nor readers are blocked by a parallel compaction.
- The read and write amplification can be fine-tuned by configuring a **Compaction Strategy**,
  either globally or on a per-tree basis.

**TL;DR:** B-Trees use random writes and synchronous operations. LSM trees use immutable data
structures, sequential writes with compression, and periodically clean up and reorganize
themselves in the background.

### Q: Does it support SQL?

No. LSM4K is a library that attempts to do **one** thing and do it well.
It is entirely possible to use LSM4K as a **building block** to create a
Database Management System (DBMS) which is capable of processing SQL.
In fact, pretty much every modern relational DBMS has a piece in its
architecture which is the "Storage Engine" - a Key-Value-Store.
LSM4K fits exactly into that role, but does not provide the rest of the DBMS.

### Q: Why another LSM implementation?

Curiosity, plus the absence of a generic JVM-based LSM implementation that
suited my own requirements. I wanted to create a Key-Value-Store with good
performance that is easy to use and really robust so it can "survive" even
in the most dire circumstances. For an excellent battle-tested implementation
which is based on B-Trees instead of LSM Trees,
see [Jetbrains Xodus](https://github.com/JetBrains/xodus).

### Q: Why the JVM and not native?

There are great LSM engines out there when working in native environments.
First and foremost, [RocksDB](https://github.com/facebook/rocksdb) deserves
a mention here. The original [LevelDB](https://github.com/google/leveldb)
was an inspiration as well. The main issue is not inherently with these
libraries, they are great pieces of software. The issues arise when you
try to access them from your JVM-based application via JNI. While the
overhead of a single JNI call may be very small and basically negligible,
when working directly with a Key-Value Store Engine, you need to do MANY
calls. For example, to create an iterator, put it to the first position
in a store, and push it all the way through the store until the end results in
roughly `N`method calls via JNI, where`N`is the number of entries in the store.
It's not uncommon to have 100.000+ entries in a key-value store. And that's
just for answering a single full-table scan on a single store. The numbers
just add up very quickly and even the tiniest of overheads will be deadly
for the application's performance.

**Q: But that approach works for SQL queries too!**

Yes, it does. Because SQL allows to formulate a very precise data demand on a
high level of abstraction. To fire a basic SQL query, you need to call the JDBC
driver just **once**. That's such a small number, even routing the call through a
network will not cause much reason for concern. The SQL database engine can then
perform the "dance of the cursors" to get exactly the data you want, and sends it
back to you. However, a Key-Value Store is (by definition) a much simpler data store
which has no notion of higher-level queries or information demands. It provides
stores over which you can iterate with cursors, and some dedicated seek capabilities,
but that's about it. You **need** to do tons and tons of method calls on it to get
the required information. And those calls need to be **fast**.

Aside from performance-related issues, it is also a matter of managing the store
at runtime. When the JVM **and** a native process (e.g. RocksDB) are involved, you
need to be very careful when assigning resources (in particular, RAM) to processes
because the JVM and the native process will inevitably compete for the available
memory. By having everything on-heap, the native side of the competition disappears
completely.

### Q: Why Kotlin?

Kotlin is an amazing tool. It compiles to the JVM just like Java, but offers
additional safety guarantees in terms of nullability. Kotlin is very pleasant to
write **and** read and overall still a much more productive language than Java itself.

### Q: Kotlin Multiplatform compatibility? Kotlin Native compatibility?

Maybe one day, but not at the moment. While LSM4K has not too many touch points with
JVM libraries, accessing files is (obviously) a must-have, and that currently happens
via the`java.io.File`API. Furthermore, LSM4K supports advanced file access methods
like`FileChannel`and`MemorySegment`, which would currently be hard to replicate in a
cross-platform fashion. Finally, LSM4K directly relies on some capabilities of Guava,
for example for caching and bloom filters.

### Q: Support for Coroutines / Async?

No. I am not a proponent of this trend at all.

### Q: Support for Virtual Threads?

No, not at the moment. LSM4K uses thread pools, and the size of these pools is used
to limit the maximum concurrent operations. Allowing an arbitrary number of operations
to hit the hard drive at the same time will ultimately lead nowhere. Virtual Threads
should not be cached and reused either, which makes controlling the maximum number of
concurrent tasks a tricky endeavour. If an actual benefit can be demonstrated,
this feature might be supported in the future.

### Q: Support for `io_uring`?

[io_uring](https://en.wikipedia.org/wiki/Io_uring) is an exciting technology provided
by the Linux kernel which allows applications to queue up read requests and get the
responses from another queue. This not only allows to minimize system calls, but it
presents the kernel with an opportunity to reorder the requests in a way that minimizes
disk I/O operations. This is a perfect fit for the Asynchronous Prefetching which is
performed by LSM4K. However, currently LSM4K does not make use of `io_uring` for two
reasons:

1. It is only available on Linux. LSM4K was written exclusively based on functionality
   which either is provided by the JVM itself (and is therefore cross-platform) or
   offers implementations for all major operating systems. `io_uring` unfortunately
   does not meet either of those criteria (yet).
2. Since its creation, `io_uring` has been found to be problematic from a
   [security point of view](https://www.upwind.io/feed/io_uring-linux-performance-boost-or-security-headache).
   In the [default docker security profile](https://github.com/moby/moby/issues/47532),
   `io_uring` is disabled entirely. 