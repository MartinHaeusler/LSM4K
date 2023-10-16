package org.chronos.chronostore.util

import com.google.common.cache.CacheBuilder
import com.google.common.cache.CacheLoader
import com.google.common.cache.LoadingCache
import com.google.common.util.concurrent.UncheckedExecutionException
import org.chronos.chronostore.util.bytes.BasicBytes
import org.chronos.chronostore.util.bytes.Bytes
import java.io.InputStream
import java.io.OutputStream

class StoreId: Comparable<StoreId> {

    companion object {

        /** Small cache, primarily to ensure that StoreIds are being deduplicated. */
        private val CACHE: LoadingCache<String, StoreId> = CacheBuilder.newBuilder().maximumSize(256).build(CacheLoader.from(::StoreId))

        fun of(vararg path: String): StoreId {
            require(path.isNotEmpty()) { "Cannot create StoreName from empty path, at least one element is required!" }
            try{
                return CACHE.get(path.joinToString(separator = PATH_SEPARATOR))
            }catch(e: UncheckedExecutionException){
                // unwrap the unchecked execution exception
                throw e.cause ?: e
            }
        }

        fun of(elements: List<String>): StoreId {
            require(elements.isNotEmpty()) { "Cannot create StoreName from empty path, at least one element is required!" }
            try{
                return CACHE.get(elements.joinToString(separator = PATH_SEPARATOR))
            }catch(e: UncheckedExecutionException){
                // unwrap the unchecked execution exception
                throw e.cause ?: e
            }
        }

        fun readFrom(input: InputStream): StoreId {
            val bytes = PrefixIO.readBytes(input)
            return readFrom(bytes)
        }

        fun readFrom(bytes: Bytes): StoreId {
            val string = bytes.asString()
            return of(string)
        }

        private const val PATH_SEPARATOR_CHAR = '/'

        private const val PATH_SEPARATOR = "${PATH_SEPARATOR_CHAR}"

        /** File names which are reserved and must not be used (mostly due to restrictions on Microsoft Windows). */
        private val FORBIDDEN_PATH_NAMES = setOf(
            "con",
            "prn",
            "aux",
            "nul",
            "com0",
            "com1",
            "com2",
            "com3",
            "com4",
            "com5",
            "com6",
            "com7",
            "com8",
            "com9",
            "lpt0",
            "lpt1",
            "lpt2",
            "lpt3",
            "lpt4",
            "lpt5",
            "lpt6",
            "lpt7",
            "lpt8",
            "lpt9",
        )

        /**
         * Pattern for checking that a single character in a path is valid.
         *
         * **Fact:** does not include whitespace.
         * **Reason:** gets rid of trouble with leading / trailing whitespace.
         *
         * **Fact:** does not permit uppercase characters.
         * **Reason:** Some file systems distinguish `foo.txt` from `FOO.txt` and others don't. By only allowing lowercase, we avoid this issue.
         *
         * **Fact:** does not permit dots, forward slashes, backslashes, ...
         * **Reason:** some operating systems  / file systems do not permit these characters while others do. This subset should be supported by all of them.
         *
         * **Fact:** permits underscore.
         * **Reason:** since we don't have whitespace or uppercase, long names will use `snake_case` or `kebap-case`. The system namespace is `_system_`.
         */
        private const val VALID_CHAR_PATTERN = "[a-z0-9-_]"

        /**
         * Regular expression for validating that the given path is a valid store path.
         *
         * Please note that the regular expression has been carefully designed such that each folder has exactly one **single** path that leads to it.
         *
         * **Fact**: Does not permit the empty path.
         * **Reason**: No empty store names are allowed.
         *
         * **Fact**: Permits forward slash (/)
         * **Reason**: Slashes are used to create subdirectories in the file system for organization purposes.
         *
         * **Fact**: Backward slashes (\) are not allowed.
         * **Reason:** We use the same separator character on all file systems and operating systems. Creating the corresponding actual file path is the task of the store manager.
         *
         * **Fact**: Multiple forward slashes in succession (//) are not allowed.
         * **Reason:** This would result in empty folder names, or in multiple different paths matching to the same folder. Neither is allowed.
         */
        private val VALID_PATH_REGEX = "${VALID_CHAR_PATTERN}+(${PATH_SEPARATOR}${VALID_CHAR_PATTERN}+)*".toRegex()

    }

    val path: List<String>

    // for efficiency reasons, we also store the full string representation.
    private val stringRepresentation: String

    // for efficiency reasons, we cache the hashCode which is also used in equals() as a preliminary check
    private val hashCode: Int

    @Suppress("ConvertSecondaryConstructorToPrimary")
    private constructor(path: String) {
        require(VALID_PATH_REGEX.matches(path)) {
            // big question here now is *why* the path is not allowed.
            val messageDetail = when {
                path.isEmpty() -> "The empty path is not allowed."
                path.contains("${PATH_SEPARATOR}${PATH_SEPARATOR}") -> "Successive path separators (${PATH_SEPARATOR}${PATH_SEPARATOR}) are not allowed."
                else -> "Only lowercase letters (a-z), digits (0-9), minus (-) and underscore (_) are allowed, optionally separated by '${PATH_SEPARATOR}'. Given path was: '${path}'"
            }
            "The given path is not a valid Store Id. ${messageDetail} Path was: '${path}'"
        }
        this.path = path.split(PATH_SEPARATOR_CHAR)
        this.stringRepresentation = path
        require(this.path.none(FORBIDDEN_PATH_NAMES::contains)) {
            val offending = this.path.first(FORBIDDEN_PATH_NAMES::contains)
            "The given path is not a valid Store Id. The path '${offending}' is reserved. Path was: '${path}'"
        }
        this.hashCode = this.stringRepresentation.hashCode()
    }

    /** Checks if this StoreId refers to a system-internal store. */
    val isSystemInternal: Boolean
        get() = this.path.first().startsWith("_")

    /** Converts this StoreId to its binary representation. */
    fun toBytes(): Bytes {
        return BasicBytes(this.stringRepresentation)
    }

    /**
     * Writes this StoreId to the given [outputStream].
     *
     * To read a StoreId from an [InputStream], please use the static method [readFrom].
     *
     * @param outputStream The output stream to write the data to.
     */
    fun writeTo(outputStream: OutputStream){
        return PrefixIO.writeBytes(outputStream, this.toBytes())
    }

    override fun compareTo(other: StoreId): Int {
        return this.stringRepresentation.compareTo(other.stringRepresentation)
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as StoreId

        if(this.hashCode != other.hashCode){
            return false
        }

        if(this.path.size != other.path.size){
            return false
        }

        return stringRepresentation == other.stringRepresentation
    }

    override fun hashCode(): Int {
        return this.hashCode
    }

    override fun toString(): String {
        return stringRepresentation
    }



}