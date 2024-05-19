# ChronoStore Compaction

## Problem Statement

We treat IOPS as a valauable system resource. We want to maximize the utility of each IO operation.

- Writing data accumulates files on disk.
- As the number of files increases, we have to search through more files to find our key-value pairs.
- As the number of files increases, reads get more and more expensive, the IOPS utilization decreases.
- We have to invest IOPS into compaction whichreduces the number of files. The IOPS we spend for this
  do not contribute directly towards our goal of fast reads, but they facilitate fast reads in the future.

## Definitions

Let $S$ be the set of all files in a store. 

Let $timestamp(f)$ denote the commit timestamp of any file.

Let $size(f)$ denote the on-disk size of any file (in bytes).

## Pressure

In contrast to other algorithms which use a fixed scheme for deciding which files to merge (layered
compaction, tiered compaction) we want a more flexible approach based on calculus and heuristics.

A central term is **Pressure**. A high pressure means that a store is dealing with many files and is
"under pressure" to compact them soon to maintain high read throughput. In contrast to that, a store
with few files is said to have "low pressure" as it doesn't need to run further compactions to achieve
high read throughput. Pressure is therefore defined as:

> $Pressure(S) = max(\#(S) - t, 0)$

Where $t$ is a configurable **threshold** for how many files we want to "accept" in the store. The
reasoning for this threshold is that compacting a store to one file isn't advisable, as compactions
will become very costly and the benefit for read operations drops sharply as the number of files in
the store approaches 1.

Note that

## Compaction Groups

Store files cannot be compacted together arbitrarily. A group of files is a valid compaction group $G$ if and only if:

> - $G \subseteq S$
> - $\#(G) > 1$
> - $\forall x \in G, \forall y \in G, \not \exists f \in S, f \not \in G: timestamp(f) \ge timestamp(x) \wedge timestamp(f) \le timestamp(y)$

In other words, we may **not** compact a group of files if there is another
file in the store which has a timestamp between the minimum timestamp and the maximum timestamp of the
files in the group. In other words, a Compaction Group may only contain files which are **successors**
of one another in the temporal ordering.

Let $g$ be the file which results from compacting all files in $G$. We define:

> $timestamp(g) = max(timestamp(f)) \forall f \in G$

## Applying Compaction

Let

> S' = $compact(S, G)$

denote the compaction of the files in the store where:

- $S$ is the set of files in the store before applying the compaction
- $G$ is the set of files which have been selected for compaction
- $S'$ is the set of files which will be in the store after applying the compaction

Note that:

> $\#(S') = \#(S) - \#(G) + 1$

In other words, the number of files in the store after the compaction will be the number of files
in the store before the compaction, minus the number of files in the compaction group, plus one
(which is the file produced by the compaction).

## Pressure Reduction

The main aim of the compaction algorithm is to **reduce pressure** on the store. The pressure reduction $R$ is defined as

> $R(G) = Pressure(compact(S, G)) - Pressure(S)$

## Cost of Compaction

The cost of the compaction is proportional to the size of the files we have to read and write. We
therefore define the $cost$ of compacting a group $G$ as:

> $cost(G) = \underset{f \in G}{\sum} size(f)$

## Selecting the most suitable Compaction Group

Let $score(G)$ denete the artificial "score" for a Compaction Group $G$. We will use this score for
ranking all possible compaction groups. The intuition behind the score is that it reflects a
cost-to-effect ratio between the achieved reduction in files and the invested IOPS.

> $score(G) = R(G) / cost(G)$

The most suitable compaction group to execute $G'$ is the group with the best cost-to-effect ratio, i.e.:

> $G' = \underset{G,\ score(G) \gt 0}{\mathrm{argmax}}(score(G))$

We also demand that $score(G')$ must be $\gt 0$ in order to be executed. Groups may
have a score of 0 if there are not enough files in the store to reach the threshold. Note that this
equation may be unsolvable, i.e. there may not be a $G$ which satisfies $score(G) > 0$. In this case,
**no compaction** will be executed in this iteration.