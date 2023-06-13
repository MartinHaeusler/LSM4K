package org.chronos.chronostore.util.sequence

import com.google.common.collect.Iterators
import org.chronos.chronostore.model.command.Command

class LatestVersionOnlyIterator(
    inner: Iterator<Command>,
) : Iterator<Command> {

    private val peekingIterator = Iterators.peekingIterator(inner)

    private var next: Command? = null

    init {
        this.moveNext()
    }

    private fun moveNext() {
        if (!this.peekingIterator.hasNext()) {
            // not found
            this.next = null
            return
        }
        var next = this.peekingIterator.next()
        while (this.peekingIterator.hasNext()) {
            val peeked = this.peekingIterator.peek()
            if (next.key == peeked.key) {
                // the versions are ordered in ascending timestamp order,
                // so we can be sure that the peeked version is newer!
                next = this.peekingIterator.next()
            } else {
                // we are at the latest version of the key
                break
            }
        }
        this.next = next
    }

    override fun hasNext(): Boolean {
        return this.next != null
    }

    override fun next(): Command {
        if (!this.hasNext()) {
            throw NoSuchElementException("Iterator is exhausted")
        }
        val result = this.next!!
        this.moveNext()
        return result
    }

}