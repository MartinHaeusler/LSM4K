package org.chronos.chronostore.util

/**
 * This class offers a context object which can hold on to [AutoCloseable]s which get cleaned up when the context is closed.
 *
 * You can obtain an instance by calling the [using] method. The intended pattern is:
 *
 * ```
 * using {
 *    // create some resources and call "autoClose()" on them
 *    val myResource1 = openSomethingCloseable().autoClose()
 *    val myResource2 = openSomethingElseCloseable().autoClose()
 *
 *    // ... do something useful with the resources ...
 *
 * } // resources get closed here (in reverse order they were registered)
 * ```
 *
 * Please note the call to [autoClose] which is contributed by the context object.
 *
 * In the example above, when we reach the end of the `using` block:
 *
 * - `myResource2` is closed first
 * - then `myResource1` is closed
 *
 * The closing happens in the **inverse** order of registration.
 *
 * If closing a resource throws an exception, the default behavior is to throw a [RuntimeException] with
 * the original exception as the cause. If multiple resources throw exceptions during the closing process,
 * further exceptions are attached to the [RuntimeException] as [suppressedExceptions]. This default
 * behavior can be adjusted by passing a [CloseExceptionHandler] to the initial [using] call.
 */
class ResourceContext(
    /** The exception handler responsible for exceptions that occur while closing resources. */
    private val closeExceptionHandler: CloseExceptionHandler,
) : AutoCloseable {

    companion object {

        /**
         * Creates a new [ResourceContext] with the given [closeExceptionHandler] and calls the given [consumer] on it.
         *
         * The intended usage pattern is:
         *
         * ```
         * using {
         *    // create some resources and call "autoClose()" on them
         *    val myResource1 = openSomethingCloseable().autoClose()
         *    val myResource2 = openSomethingElseCloseable().autoClose()
         *    // ... do something useful with the resources ...
         *
         * } // resources get closed here (in reverse order they were registered)
         * ```
         *
         * @param closeExceptionHandler Responsible for handling exceptions that occur when resources get closed. Defaults to the [DEFAULT_CLOSING_EXCEPTION_HANDLER].
         * @param consumer The action to execute within the context.
         *
         * @return The result of the [consumer].
         */
        inline fun <T> using(
            closeExceptionHandler: CloseExceptionHandler = DEFAULT_CLOSING_EXCEPTION_HANDLER,
            consumer: ResourceContext.() -> T,
        ): T {
            return ResourceContext(closeExceptionHandler).use(consumer)
        }

        /**
         * The default exception handler for the [using] function.
         *
         * If closing a resource throws an exception, this handler throws a [RuntimeException] with
         * the original exception as the cause. If multiple resources throw exceptions during the
         * closing process, further exceptions are attached to the [RuntimeException] as
         * [suppressedExceptions].
         */
        val DEFAULT_CLOSING_EXCEPTION_HANDLER = CloseExceptionHandler { resourcesAndExceptions ->
            if (resourcesAndExceptions.isEmpty()) {
                return@CloseExceptionHandler
            }
            val exceptions = resourcesAndExceptions.map { it.second }
            val ex = RuntimeException("Failed to close ${exceptions.size} registered resource(s): ${exceptions.first()}", exceptions.first())
            for (suppressed in exceptions.asSequence().drop(1)) {
                ex.addSuppressed(suppressed)
            }
            throw ex
        }
    }

    /** The list of resources which have been registered to this context. */
    private val resources = mutableListOf<AutoCloseable>()

    /** Tells if this context has already been closed or not. */
    private var closed: Boolean = false

    /**
     * Registers the receiver at this [ResourceContext], closing it when the context closes.
     *
     * @receiver The object to close when the context is closed.
     *
     * @return The receiver object.
     */
    fun <T : AutoCloseable> T.autoClose(): T {
        check(!closed){ "This using{ ... } context has already been closed!" }
        resources += this
        return this
    }

    /**
     * Closes the context and all resources attached to it.
     *
     * Resources will be closed in the **reverse** order they have been registered.
     */
    override fun close() {
        if (this.closed) {
            return
        }
        this.closed = true

        // keep track of the exceptions which are thrown while closing the resources,
        // as well as the resources that produced the exception.
        val resourcesAndExceptions = mutableListOf<Pair<AutoCloseable, Throwable>>()

        // close all resources in reverse order
        for (resource in resources.asReversed()) {
            try {
                resource.close()
            } catch (t: Throwable) {
                resourcesAndExceptions += Pair(resource, t)
            }
        }

        // did anything bad happen?
        if (resourcesAndExceptions.isEmpty()) {
            return
        }

        // at least one resource threw an exception in the close() call,
        // let the exception handler deal with it.
        this.closeExceptionHandler.handleCloseExceptions(resourcesAndExceptions)
    }

    /**
     * Exception handler that gets invoked when one or more resources throw exceptions in their respective `close()` call.
     */
    fun interface CloseExceptionHandler {

        /**
         * Invokes the handler.
         *
         * @param resourcesAndExceptions A list of pairs. The first component of each pair is the resource which
         *                              threw an exception on `close()`, the second component of the pair is said exception.
         */
        fun handleCloseExceptions(resourcesAndExceptions: List<Pair<AutoCloseable, Throwable>>)

    }
}