package org.chronos.chronostore.jmh

import org.apache.commons.math3.random.BitsStreamGenerator
import org.apache.commons.math3.random.MersenneTwister
import org.openjdk.jmh.annotations.Param
import org.openjdk.jmh.annotations.Scope
import org.openjdk.jmh.annotations.State
import org.openjdk.jmh.infra.BenchmarkParams
import java.io.File
import java.io.IOException
import java.lang.System.getProperty
import java.util.zip.CRC32


@State(Scope.Benchmark)
open class Common {

    val RND_MB = ByteArray(1048576)
    val STRING_KEY_LENGTH = 16
    private val RND: BitsStreamGenerator = MersenneTwister()
    private val S_BLKSIZE = 512 // from sys/stat.h

    private var TMP_BENCH: File? = null

    var compact: File? = null

    lateinit var crc: CRC32

    /**
     * Keys are always an integer, however they are actually stored as integers
     * (taking 4 bytes) or as zero-padded 16 byte strings. Storing keys as
     * integers offers a major performance gain.
     */
    @Param("true")
    var intKey = false

    /**
     * Determined during [.setup] based on [.intKey] value.
     */
    var keySize = 0

    /**
     * Keys in designated (random/sequential) order.
     */
    lateinit var keys: IntArray

    /**
     * Number of entries to read/write to the database.
     */
    @Param("1000000")
    var num = 0

    /**
     * Whether the keys are to be inserted into the database in sequential order
     * (and in the "readKeys" case, read back in that order). For LMDB, sequential
     * inserts use [org.lmdbjava.PutFlags.MDB_APPEND] and offer a major
     * performance gain. If this field is false, the append flag will not be used
     * and the keys will instead be inserted (and read back via "readKeys") in a
     * random order.
     */
    @Param("true")
    var sequential = false

    var tmp: File? = null

    /**
     * Whether the values contain random bytes or are simply the same as the key.
     * If true, the random bytes are obtained sequentially from a 1 MB random byte
     * buffer.
     */
    @Param("false")
    var valRandom = false

    /**
     * Number of bytes in each value.
     */
    @Param("100")
    var valSize = 0

    init {
        RND.nextBytes(RND_MB);
        val tmpParent = getProperty("java.io.tmpdir");
        TMP_BENCH = File(tmpParent, "lmdbjava-benchmark-scratch");
    }

    @Throws(IOException::class)
    open fun setup(b: BenchmarkParams) {
        keySize = if (intKey) {
            Int.SIZE_BYTES
        } else {
            STRING_KEY_LENGTH
        }
        crc = CRC32()
        val set = mutableSetOf(num)
        keys = IntArray(num)
        for (i in 0 until num) {
            if (sequential) {
                keys[i] = i
            } else {
                while (true) {
                    var candidateKey: Int = RND.nextInt()
                    if (candidateKey < 0) {
                        candidateKey *= -1
                    }
                    if (!set.contains(candidateKey)) {
                        set.add(candidateKey)
                        keys[i] = candidateKey
                        break
                    }
                }
            }
        }
        TMP_BENCH?.deleteRecursively()
        tmp = create(b, "")
        compact = create(b, "-compacted")
    }

    fun reportSpaceBeforeClose() {
        if (tmp!!.name.contains(".readKey-")) {
            reportSpaceUsed(tmp, "before-close")
        }
    }

    @Throws(IOException::class)
    open fun teardown() {
        // we only output for key, as all impls offer it and it should be fixed
        val tmp = this.tmp
        if (tmp != null && tmp.name.contains(".readKey-")) {
            reportSpaceUsed(tmp, "after-close")
        }
        TMP_BENCH?.deleteRecursively()
    }

    protected fun reportSpaceUsed(dir: File?, desc: String) {
        if(dir == null){
            return
        }
        val bytes = dir.walkTopDown().filter { it.isFile }.map { it.length() }.sum()
        println("\nBytes\t$desc\t$bytes\t${dir.name}")
    }

    fun padKey(key: Int): String {
        return key.toString().padStart(16, '0')
    }

    private fun create(b: BenchmarkParams, suffix: String): File {
        val f = File(TMP_BENCH, b.id() + suffix)
        check(f.mkdirs()) { "Cannot mkdir $f" }
        return f
    }

}