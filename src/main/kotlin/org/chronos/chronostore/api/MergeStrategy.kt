package org.chronos.chronostore.api

import org.chronos.chronostore.lsm.LSMTreeFile
import org.chronos.chronostore.util.statistics.StatisticExtensions.stdDev

enum class MergeStrategy {

    DEFAULT {

        private val MAX_GROUP_SIZE = 10_000

        override fun selectFilesToMerge(allFiles: List<LSMTreeFile>): List<LSMTreeFile> {
            // only files with adjacent timestamp ranges are valid merge groups. To find these files easily,
            // we sort by the minTimestamp. Since we assert in our LSMTree that the time ranges are non-overlapping,
            // sorting by minTimestamp is sufficient to create an ordering of ascending adjacent timestamp ranges.
            val filesAscending = allFiles.sortedBy { it.header.metaData.minTSN }

            // boosters:
            // - number of files in the group (multiplicative)
            // - similarity in number of entries
            // - min(numberOfMerges)

            val selectedGroup = filesAscending.indices.asSequence()
                .mapNotNull { this.createMergeGroup(it, filesAscending) }
                .maxByOrNull { this.computeGroupScore(it) }

            return selectedGroup ?: emptyList()
        }

        private fun createMergeGroup(
            startIndex: Int,
            files: List<LSMTreeFile>
        ): List<LSMTreeFile>? {
            val startFile = files[startIndex]
            val group = mutableListOf<LSMTreeFile>()
            var aggregatedSize = startFile.header.metaData.totalEntries

            var currentIndex = startIndex
            while (currentIndex < files.lastIndex) {
                currentIndex++

                val file = files[currentIndex]
                group += file
                aggregatedSize += file.header.metaData.totalEntries

                if (aggregatedSize >= MAX_GROUP_SIZE) {
                    // group is full, don't add another file.
                    return group
                }
            }
            // groups of size 0 or 1 are not relevant for merging.
            return group.takeIf { it.size > 1 }
        }

        private fun computeGroupScore(group: List<LSMTreeFile>): Double {
            val totalNumberOfEntries = group.sumOf { it.header.metaData.totalEntries }.coerceAtLeast(1)
            val standardDeviationInNumberOfEntries = group.asSequence().map { it.header.metaData.totalEntries }.stdDev() / totalNumberOfEntries
            val numberOfEntriesSimilarityScore = (1.0 - standardDeviationInNumberOfEntries).coerceIn(0.0, 1.0)
            val totalMerges = group.sumOf { it.header.metaData.numberOfMerges }.coerceAtLeast(1)
            val standardDeviationInNumberOfMerges = group.asSequence().map { it.header.metaData.numberOfMerges }.stdDev() / totalMerges
            val numberOfMergesSimilarityScore = (1.0 - standardDeviationInNumberOfMerges).coerceIn(0.0, 1.0)
            return (numberOfEntriesSimilarityScore + numberOfMergesSimilarityScore) * group.size
        }

    },

    ;

    abstract fun selectFilesToMerge(allFiles: List<LSMTreeFile>): List<LSMTreeFile>

}