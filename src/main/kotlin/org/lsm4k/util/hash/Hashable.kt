package org.lsm4k.util.hash

import com.google.common.hash.HashCode
import com.google.common.hash.HashFunction
import com.google.common.hash.Hasher

/**
 * An advanced hashing interface which allows for composite hashes and multiple hash functions.
 */
interface Hashable {

    /**
     * Computes the [HashCode] of this object using the given [hashFunction].
     *
     * @param hashFunction The hash function to apply.
     *
     * @return The hashcode of this object according to the given hash function.
     */
    fun hash(hashFunction: HashFunction): HashCode {
        return this.hash(hashFunction.newHasher()).hash()
    }

    /**
     * Adds the hash of this object to the given [hasher].
     *
     * @param hasher The hasher to add the data to
     *
     * @return the [hasher] for method chaining.
     */
    fun hash(hasher: Hasher): Hasher

}