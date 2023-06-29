package org.chronos.chronostore.util.cursor

import org.chronos.chronostore.util.Order


/**
 * A [Cursor] is a movable pointer to a certain entry in a list of key-value pairs.
 *
 * This list can be an in-memory collection, a keyspace on disk, or any
 * other sorted collection of key-value pairs.
 *
 * Cursors may be backed by external resources, therefore they need to be [closed][close]
 * by their creator.
 *
 * A cursor always starts at an [invalid][isValidPosition] when it is created. It merely
 * points to its target list, but at no specified location. The following methods can
 * be used to move the cursor to a valid location (they can be used on an uninitialized
 * cursor, but also on a cursor with a valid location):
 *
 * - [first] moves the cursor to the first entry in the list (which has the smallest key)
 * - [last] moves the cursor to the last entry in the list (which has the largest key)
 * - [seekExactlyOrPrevious] moves the cursor to the given key, or its largest existing predecessor if it doesn't exist.
 * - [seekExactlyOrNext] moves the cursor to the given key, or its smallest existing successor if it doesn't exist.
 *
 * All of these methods may return `false`. This usually indicates that the collection is empty.
 *
 * @author martin.haeusler89@gmail.com -- Initial Contribution and API
 */
interface Cursor<K, V> : AutoCloseable {

    /**
     * Returns the mod count of this cursor, i.e. how many moves have been executed on it.
     *
     * This is mostly used for consistency checking in cursor orchestration.
     */
    val modCount: Long

    /**
     * Checks if this cursor is still open.
     */
    val isOpen: Boolean

    /**
     * Checks if this cursor is currently at a valid position.
     *
     * If the cursor is at a valid position, [keyOrNull] will never return `null`.
     *
     * Each cursor initially starts at an undefined (thus invalid) location.
     *
     * You can use the following methods to move a cursor with an invalid location
     * to a valid one:
     *
     * - [first] moves the cursor to the first entry in the list (which has the smallest key)
     * - [last] moves the cursor to the last entry in the list (which has the largest key)
     * - [seekExactlyOrPrevious] moves the cursor to the given key, or its largest existing predecessor if it doesn't exist.
     * - [seekExactlyOrNext] moves the cursor to the given key, or its smallest existing successor if it doesn't exist.
     *
     * All of these methods may return `false`. This usually indicates that the collection is empty. If that
     * happens, [isValidPosition] will still be `false` after the call.
     */
    val isValidPosition: Boolean

    /**
     * Invalidates the position of this cursor.
     *
     * Until re-validation (e.g. via [first] or [last]), this will cause:
     * - [isValidPosition] to return `false`
     * - [keyOrNull] and [valueOrNull] to return `null`
     * - [key] and [value] to throw [IllegalStateException]s
     * - [next] and [previous] to return `false`
     *
     */
    fun invalidatePosition()

    /**
     * Moves the cursor such that it points to the first key-value pair (which has the smallest key).
     *
     * @return `true` if the move was successful, `false` if the collection is empty.
     *
     * @see firstOrThrow
     * @see last
     * @see lastOrThrow
     */
    fun first(): Boolean

    /**
     * Moves the cursor such that it points to the first key-value pair (which has the smallest key).
     *
     * @throws IllegalStateException if the move didn't succeed (e.g. because the collection is empty).
     *
     * @see first
     * @see last
     * @see lastOrThrow
     */
    fun firstOrThrow() {
        if (!this.first()) {
            throw IllegalStateException("Illegal Iterator state - move 'first()' failed!")
        }
    }

    /**
     * Moves the cursor such that it points to the last key-value pair (which has the largest key).
     *
     * @return `true` if the move was successful, `false` if the collection is empty.
     *
     * @see first
     * @see firstOrThrow
     * @see lastOrThrow
     */
    fun last(): Boolean

    /**
     * Moves the cursor such that it points to the last key-value pair (which has the largest key).
     *
     * @throws IllegalStateException if the move didn't succeed (e.g. because the collection is empty).
     *
     * @see first
     * @see firstOrThrow
     * @see last
     */
    fun lastOrThrow() {
        if (!this.last()) {
            throw IllegalStateException("Illegal Iterator state - move 'last()' failed!")
        }
    }

    /**
     * Moves the cursor to the next entry (in key order).
     *
     * If this method returns `false` (because there is no next key to go to), then the
     * cursor will remain in its current position.
     *
     * If [isValidPosition] is `false`, this method will always return `false` by definition.
     *
     * @return `true` if the move was successful, `false` if there is no next key.
     */
    fun next(): Boolean

    /**
     * Moves the cursor to the next entry (in key order).
     *
     * If this method throws an [IllegalStateException] (because there is no next key to go to), then the
     * cursor will remain in its current position.
     *
     * If [isValidPosition] is `false`, this method will always throw an exception by definition.
     *
     * @throws IllegalStateException if there is no next entry or the current cursor position is invalid.
     */
    fun nextOrThrow() {
        if (!this.next()) {
            throw IllegalStateException("Illegal Iterator state - move 'next()' failed!")
        }
    }

    /**
     * Moves the cursor to the previous entry (in key order).
     *
     * If this method returns `false` (because there is no previous key to go to), then the
     * cursor will remain in its current position.
     *
     * If [isValidPosition] is `false`, this method will always return `false` by definition.
     *
     * @return `true` if the move was successful, `false` if there is no previous key.
     */
    fun previous(): Boolean

    /**
     * Moves the cursor to the previous entry (in key order).
     *
     * If this method throws an [IllegalStateException] (because there is no previous key to go to), then the
     * cursor will remain in its current position.
     *
     * If [isValidPosition] is `false`, this method will always throw an exception by definition.
     *
     * @throws IllegalStateException if there is no previous entry or the current cursor position is invalid.
     */
    fun previousOrThrow() {
        if (!this.previous()) {
            throw IllegalStateException("Illegal Iterator state - move 'prev()' failed!")
        }
    }

    /**
     * Moves this cursor in the given direction.
     *
     * This is a convenience method. Using [Order.ASCENDING] will call [next],
     * using [Order.DESCENDING] will call [previous]. The result of the respective
     * call will be returned.
     *
     * @param direction The direction to move in.
     *
     * @return The result of the move. If the result is `true`, the cursor was successfully moved
     * into the given direction. If the result is `false`, the cursor was unable to move in the
     * given direction and remained in its previous location. If the cursor position was
     * [invalid][isValidPosition] before calling this method, the result will always be `false`
     * and the cursor will remain in an invalid position.
     */
    fun move(direction: Order): Boolean {
        return when (direction) {
            Order.ASCENDING -> this.next()
            Order.DESCENDING -> this.previous()
        }
    }

    /**
     * Moves this cursor in the given direction.
     *
     * This is a convenience method. Using [Order.ASCENDING] will call [nextOrThrow],
     * using [Order.DESCENDING] will call [previousOrThrow].
     *
     * @param direction The direction to move in.
     *
     * @throws IllegalStateException if the move was not possible or the current cursor position is invalid.
     */
    fun moveOrThrow(direction: Order) {
        when (direction) {
            Order.ASCENDING -> this.nextOrThrow()
            Order.DESCENDING -> this.previousOrThrow()
        }
    }

    /**
     * Performs a look-ahead to the next element.
     *
     * If this cursor is in an [invalid][isValidPosition] position, `null` will be returned.
     * If there is no next element (because the cursor is currently located at the last element), `null` will be returned.
     *
     * No matter which result is returned, the (observable) current position of the cursor does not change.
     *
     * @return The next entry in the store (or `null` if there is none) without moving this cursor.
     */
    fun peekNext(): Pair<K, V>? {
        if (!this.isValidPosition) {
            return null
        }
        if (!this.next()) {
            return null
        }
        val entry = this.key to this.value
        this.previousOrThrow()
        return entry
    }

    /**
     * Performs a look-behind to the previous element.
     *
     * If this cursor is in an [invalid][isValidPosition] position, `null` will be returned.
     * If there is no previous element (because the cursor is currently located at the first element), `null` will be returned.
     *
     * No matter which result is returned, the (observable) current position of the cursor does not change.
     *
     * @return The previous entry in the store (or `null` if there is none) without moving this cursor.
     */
    fun peekPrevious(): Pair<K, V>? {
        if (!this.isValidPosition) {
            return null
        }
        if (!this.previous()) {
            return null
        }
        val entry = this.key to this.value
        this.nextOrThrow()
        return entry
    }

    /**
     * Performs a look-ahead or look-behind while maintaining the current cursor position.
     *
     * If this cursor is in an [invalid][isValidPosition] position, `null` will be returned.
     * If there is no element in the given [direction] (because we've reached the first / last element of the store), `null` will be returned.
     *
     * No matter which result is returned, the (observable) current position of the cursor does not change.
     *
     * @param direction The direction to move to. [Order.ASCENDING] performs [peekNext], [Order.DESCENDING] performs [peekPrevious].
     *
     * @return The next entry in the given direction (or `null` if there is none) without moving this cursor.
     */
    fun peek(direction: Order): Pair<K, V>? {
        return when(direction){
            Order.ASCENDING -> peekNext()
            Order.DESCENDING -> peekPrevious()
        }
    }

    /**
     * Returns the key of the entry the cursor is pointing to, or `null` if the cursor position [is invalid][isValidPosition].
     */
    val keyOrNull: K?

    /**
     *  Returns the key of the entry the cursor is pointing to.
     *
     *  @throws IllegalStateException if the cursor position [is invalid][isValidPosition].
     */
    val key: K
        get() {
            if (!this.isValidPosition) {
                throw IllegalStateException("Illegal Iterator state - called 'key' on undefined iterator position!")
            }
            return this.keyOrNull
                ?: throw IllegalStateException("Illegal Iterator state - called 'key' on a valid iterator position, but got NULL!")
        }

    /**
     * Returns the value of the entry the cursor is pointing to, or `null` if the cursor position [is invalid][isValidPosition].
     */
    val valueOrNull: V?

    /**
     *  Returns the value of the entry the cursor is pointing to.
     *
     *  @throws IllegalStateException if the cursor position [is invalid][isValidPosition].
     */
    val value: V
        get() {
            if (!this.isValidPosition) {
                throw IllegalStateException("Illegal Iterator state - called 'value' on undefined iterator position!")
            }
            return this.valueOrNull
                ?: throw IllegalStateException("Illegal Iterator state - called 'value' on a valid iterator position, but got NULL!")
        }

    /**
     * Moves the cursor to the greatest entry which has a key that is **strictly smaller** than the given [key].
     *
     * @param key The key to look for.
     *
     * @return `true` if a suitable entry was found and the cursor is at a valid position, otherwise `false`.
     */
    fun seekStrictlyPrevious(key: K): Boolean {
        if (!this.seekExactlyOrPrevious(key)) {
            return false
        }
        val foundKey = this.key
        val exactMatch = if (foundKey is ByteArray && key is ByteArray) {
            key.contentEquals(foundKey)
        } else {
            key == foundKey
        }
        return if (exactMatch) {
            if (this.previous()) {
                true
            } else {
                this.invalidatePosition()
                false
            }
        } else {
            true
        }
    }

    /**
     * Moves the cursor to the smallest entry which has a key that is **strictly greater** than the given [key].
     *
     * @param key The key to look for.
     *
     * @return `true` if a suitable entry was found and the cursor is at a valid position, otherwise `false`.
     */
    fun seekStrictlyNext(key: K): Boolean {
        if (!this.seekExactlyOrNext(key)) {
            return false
        }
        val foundKey = this.key
        val exactMatch = if (foundKey is ByteArray && key is ByteArray) {
            key.contentEquals(foundKey)
        } else {
            key == foundKey
        }
        return if (exactMatch) {
            if (this.next()) {
                true
            } else {
                this.invalidatePosition()
                false
            }
        } else {
            true
        }
    }

    /**
     * Moves the cursor to the given [key], or its largest existing predecessor if the given [key] doesn't exist.
     *
     * If this method returns `false`, the seek has failed and there is no exact-or-previous match. In this case,
     * calls to [isValidPosition] will return `false` and the cursor position will be undefined.
     *
     * @return `true` if the seek was successful and the cursor has a defined position (at or before the given [key]),
     * or `false` if the seek failed because there was no key less than or equal to the given [key].
     */
    fun seekExactlyOrPrevious(key: K): Boolean

    /**
     * Moves the cursor to the given [key], or its smallest existing successor if the given [key] doesn't exist.
     *
     * If this method returns `false`, the seek has failed and there is no exact-or-greater match. In this case,
     * calls to [isValidPosition] will return `false` and the cursor position will be undefined.
     *
     * @return `true` if the seek was successful and the cursor has a defined position (at or after the given [key]),
     * or `false` if the seek failed because there was no key less than or equal to the given [key].
     */
    fun seekExactlyOrNext(key: K): Boolean

    /**
     * Creates a [Sequence] view on this cursor that iterates over the ascending [key]s and [value]s, starting from the current position.
     *
     * Iterating over the resulting sequence will also move the cursor accordingly. The sequence can only be
     * used once for iteration. Moving the cursor manually while iterating over the sequence leads to undefined behavior.
     *
     * If [isValidPosition] returns `false`, this method will return an empty sequence.
     *
     * @param includeCurrent Use `true` (default) to include the entry the cursor is currently pointing to, or `false` to exclude it.
     *
     * @return The sequence of entries in ascending order.
     */
    fun ascendingEntrySequenceFromHere(includeCurrent: Boolean = true): Sequence<Pair<K, V>> {
        if (!this.isValidPosition) {
            return emptySequence()
        }
        val start = if (!includeCurrent) {
            emptySequence()
        } else {
            sequenceOf(this.key to this.value)
        }
        return start + generateSequence {
            if (this.next()) {
                this.key to this.value
            } else {
                null
            }
        }
    }


    /**
     * Creates a [Sequence] view on this cursor that iterates over the descending [key]s and [value]s, starting from the current position.
     *
     * Iterating over the resulting sequence will also move the cursor accordingly. The sequence can only be
     * used once for iteration. Moving the cursor manually while iterating over the sequence leads to undefined behavior.
     *
     * If [isValidPosition] returns `false`, this method will return an empty sequence.
     *
     * @param includeCurrent Use `true` (default) to include the entry the cursor is currently pointing to, or `false` to exclude it.
     *
     * @return The sequence of entries in descending order.
     */
    fun descendingEntrySequenceFromHere(includeCurrent: Boolean = true): Sequence<Pair<K, V>> {
        if (!this.isValidPosition) {
            return emptySequence()
        }
        val start = if (!includeCurrent) {
            emptySequence()
        } else {
            sequenceOf(this.key to this.value)
        }
        return start + generateSequence {
            if (this.previous()) {
                this.key to this.value
            } else {
                null
            }
        }
    }

    /**
     * Creates a [Sequence] view on this cursor that iterates over the ascending [value]s, starting from the current position.
     *
     * Iterating over the resulting sequence will also move the cursor accordingly. The sequence can only be
     * used once for iteration. Moving the cursor manually while iterating over the sequence leads to undefined behavior.
     *
     * If [isValidPosition] returns `false`, this method will return an empty sequence.
     *
     * @param includeCurrent Use `true` (default) to include the entry the cursor is currently pointing to, or `false` to exclude it.
     *
     * @return The sequence of values in ascending order.
     */
    fun ascendingValueSequenceFromHere(includeCurrent: Boolean = true): Sequence<V> {
        if (!this.isValidPosition) {
            return emptySequence()
        }
        val start = if (!includeCurrent) {
            emptySequence()
        } else {
            sequenceOf(this.value)
        }
        return start + generateSequence {
            if (this.next()) {
                this.value
            } else {
                null
            }
        }
    }

    /**
     * Creates a [Sequence] view on this cursor that iterates over the ascending [key]s, starting from the current position.
     *
     * Iterating over the resulting sequence will also move the cursor accordingly. The sequence can only be
     * used once for iteration. Moving the cursor manually while iterating over the sequence leads to undefined behavior.
     *
     * If [isValidPosition] returns `false`, this method will return an empty sequence.
     *
     * @param includeCurrent Use `true` (default) to include the entry the cursor is currently pointing to, or `false` to exclude it.
     *
     * @return The sequence of keys in ascending order.
     */
    fun ascendingKeySequenceFromHere(includeCurrent: Boolean = true): Sequence<K> {
        if (!this.isValidPosition) {
            return emptySequence()
        }
        val start = if (!includeCurrent) {
            emptySequence()
        } else {
            sequenceOf(this.key)
        }
        return start + generateSequence {
            if (this.next()) {
                this.key
            } else {
                null
            }
        }
    }

    /**
     * Creates a [Sequence] view on this cursor that iterates over the descending [value]s, starting from the current position.
     *
     * Iterating over the resulting sequence will also move the cursor accordingly. The sequence can only be
     * used once for iteration. Moving the cursor manually while iterating over the sequence leads to undefined behavior.
     *
     * If [isValidPosition] returns `false`, this method will return an empty sequence.
     *
     * @param includeCurrent Use `true` (default) to include the entry the cursor is currently pointing to, or `false` to exclude it.
     *
     * @return The sequence of values in descending order.
     */
    fun descendingValueSequenceFromHere(includeCurrent: Boolean = true): Sequence<V> {
        if (!this.isValidPosition) {
            return emptySequence()
        }
        val start = if (!includeCurrent) {
            emptySequence()
        } else {
            sequenceOf(this.value)
        }
        return start + generateSequence {
            if (this.previous()) {
                this.value
            } else {
                null
            }
        }
    }


    /**
     * Creates a [Sequence] view on this cursor that iterates over the descending [key]s, starting from the current position.
     *
     * Iterating over the resulting sequence will also move the cursor accordingly. The sequence can only be
     * used once for iteration. Moving the cursor manually while iterating over the sequence leads to undefined behavior.
     *
     * If [isValidPosition] returns `false`, this method will return an empty sequence.
     *
     * @param includeCurrent Use `true` (default) to include the entry the cursor is currently pointing to, or `false` to exclude it.
     *
     * @return The sequence of keys in descending order.
     */
    fun descendingKeySequenceFromHere(includeCurrent: Boolean = true): Sequence<K> {
        if (!this.isValidPosition) {
            return emptySequence()
        }
        val start = if (!includeCurrent) {
            emptySequence()
        } else {
            sequenceOf(this.key)
        }
        return start + generateSequence {
            if (this.previous()) {
                this.key
            } else {
                null
            }
        }
    }

    /**
     * Creates a list of all keys in this cursor, in ascending order.
     *
     * The cursor will be automatically positioned at the beginning
     * of the store, and a full iteration will be performed. Please
     * note that the cursor will be positioned at the last entry of
     * the store after this operation concludes. If the store was
     * empty, the cursor position will be invalid instead.
     *
     * @return The full list of keys in this cursor, in ascending order.
     */
    fun listAllKeysAscending(): List<K> {
        if (!this.first()) {
            this.invalidatePosition()
            return emptyList()
        }
        val sequence = this.ascendingKeySequenceFromHere(includeCurrent = true)
        return sequence.toList()
    }

    /**
     * Creates a list of all keys in this cursor, in descending order.
     *
     * The cursor will be automatically positioned at the end
     * of the store, and a full backwards iteration will be performed. Please
     * note that the cursor will be positioned at the first entry of
     * the store after this operation concludes. If the store was
     * empty, the cursor position will be invalid instead.
     *
     * @return The full list of keys in this cursor, in descending order.
     */
    fun listAllKeysDescending(): List<K> {
        if (!this.last()) {
            this.invalidatePosition()
            return emptyList()
        }
        val sequence = this.descendingKeySequenceFromHere(includeCurrent = true)
        return sequence.toList()
    }

    /**
     * Creates a list of all values in this cursor, in ascending key order.
     *
     * The cursor will be automatically positioned at the beginning
     * of the store, and a full iteration will be performed. Please
     * note that the cursor will be positioned at the last entry of
     * the store after this operation concludes. If the store was
     * empty, the cursor position will be invalid instead.
     *
     * @return The full list of values in this cursor, in ascending key order.
     */
    fun listAllValuesAscending(): List<V> {
        if (!this.first()) {
            this.invalidatePosition()
            return emptyList()
        }
        val sequence = this.ascendingValueSequenceFromHere(includeCurrent = true)
        return sequence.toList()
    }

    /**
     * Creates a list of all values in this cursor, in descending key order.
     *
     * The cursor will be automatically positioned at the end
     * of the store, and a full backwards iteration will be performed. Please
     * note that the cursor will be positioned at the first entry of
     * the store after this operation concludes. If the store was
     * empty, the cursor position will be invalid instead.
     *
     * @return The full list of values in this cursor, in descending key order.
     */
    fun listAllValuesDescending(): List<V> {
        if (!this.last()) {
            this.invalidatePosition()
            return emptyList()
        }
        val sequence = this.descendingValueSequenceFromHere(includeCurrent = true)
        return sequence.toList()
    }

    /**
     * Creates a list of all entries in this cursor, in ascending key order.
     *
     * The cursor will be automatically positioned at the beginning
     * of the store, and a full iteration will be performed. Please
     * note that the cursor will be positioned at the last entry of
     * the store after this operation concludes. If the store was
     * empty, the cursor position will be invalid instead.
     *
     * @return The full list of entries in this cursor, in ascending key order.
     */
    fun listAllEntriesAscending(): List<Pair<K, V>> {
        if (!this.first()) {
            this.invalidatePosition()
            return emptyList()
        }
        val sequence = this.ascendingEntrySequenceFromHere(includeCurrent = true)
        return sequence.toList()
    }

    /**
     * Creates a list of all entries in this cursor, in descending key order.
     *
     * The cursor will be automatically positioned at the end
     * of the store, and a full backwards iteration will be performed. Please
     * note that the cursor will be positioned at the first entry of
     * the store after this operation concludes. If the store was
     * empty, the cursor position will be invalid instead.
     *
     * @return The full list of entries in this cursor, in descending key order.
     */
    fun listAllEntriesDescending(): List<Pair<K, V>> {
        if (!this.last()) {
            this.invalidatePosition()
            return emptyList()
        }
        val sequence = this.descendingEntrySequenceFromHere(includeCurrent = true)
        return sequence.toList()
    }

    /**
     * Creates a view on this cursor where the keys are mapped according to the given functions.
     *
     * The [map] function must be **bijective** and [mapInverse] must be its inverse.
     *
     * As the returned object is a view on this cursor, any movement on this cursor will be reflected
     * in the view, and vice versa.
     *
     * @param map Maps a key to the desired target format.
     * @param mapInverse Maps a given target format key back to its original format.
     *
     * @return A view on this cursor with the given key mapping applied. Position changes on this
     * cursor will be visible on the view, and vice versa.
     */
    fun <MK> mapKey(map: (K) -> MK, mapInverse: (MK) -> (K)): Cursor<MK, V> {
        return MappingCursor(original = this, mapKey = map, mapKeyInverse = mapInverse, mapValue = { it })
    }

    /**
     * Creates a view on this cursor where the values are mapped according to the given function.
     *
     * As the returned object is a view on this cursor, any movement on this cursor will be reflected
     * in the view, and vice versa.
     *
     * @param map Maps a value to the desired target format.
     *
     * @return A view on this cursor with the given value mapping applied. Position changes on this
     * cursor will be visible on the view, and vice versa.
     */
    fun <MV> mapValue(map: (V) -> MV): Cursor<K, MV> {
        return MappingCursor(original = this, mapKey = { it }, mapKeyInverse = { it }, mapValue = map)
    }

    /**
     * Creates a view on this cursor where the keys are mapped according to the given functions.
     *
     * The [mapKey] function must be **bijective** and [mapKeyInverse] must be its inverse.
     *
     * As the returned object is a view on this cursor, any movement on this cursor will be reflected
     * in the view, and vice versa.
     *
     * @param mapKey Maps a key to the desired target format.
     * @param mapKeyInverse Maps a given target format key back to its original format.
     * @param mapValue Maps a value to the desired target format.
     *
     * @return A view on this cursor with the given key-value mapping applied. Position changes on this
     * cursor will be visible on the view, and vice versa.
     */
    fun <MK, MV> mapKeyAndValue(mapKey: (K) -> MK, mapKeyInverse: (MK) -> (K), mapValue: (V) -> MV): Cursor<MK, MV> {
        return MappingCursor(original = this, mapKey = mapKey, mapKeyInverse = mapKeyInverse, mapValue = mapValue)
    }

    /**
     * Creates a new cursor which takes ownership of this cursor and returns only entries which match the given key filter.
     *
     * @param isValidKey The filter to apply to the keys.
     *
     * @return A view on this cursor with the given filter applied. The returned cursor takes ownership of this cursor.
     */
    fun filterKeys(isValidKey: (K) -> Boolean): Cursor<K, V> {
        return KeyFilteringCursor(this, isValidKey)
    }

    /**
     * Creates a new cursor which takes ownership of this cursor and returns only entries which match the given value filter.
     *
     * @param isValidValue The filter to apply to the values. Please note that the result of [Cursor.valueOrNull] is passed to the filter, so it may receive `null` as input.
     *
     * @return A view on this cursor with the given filter applied. The returned cursor takes ownership of this cursor.
     */
    fun filterValues(isValidValue: (V?) -> Boolean): Cursor<K, V> {
        return ValueFilteringCursor(this, isValidValue)
    }

    /**
     * Wraps this cursor into another one which performs the given action when it is [closed][close].
     *
     * @param action The action to perform on close.
     */
    fun onClose(action: () -> Unit): Cursor<K, V> {
        return CursorWithCloseHandler(this, action)
    }

}