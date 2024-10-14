package org.chronos.chronostore.api.exceptions

open class CursorException : ChronoStoreException {

    companion object {

        fun throwIfExceptionsArePresent(vararg exceptions: Throwable?) {
            throwIfExceptionsArePresent(exceptions.filterNotNull())
        }

        fun throwIfExceptionsArePresent(exceptions: List<Throwable>?) {
            if (exceptions.isNullOrEmpty()) {
                return
            }

            // the causes may be ChronoStoreCursorExceptions themselves.
            // No need to keep these, resolve their causes.
            val allExceptions = exceptions.flatMap { e ->
                if (e is CursorException) {
                    val causes = e.suppressedExceptions + listOfNotNull(e.cause)
                    causes.ifEmpty { listOf(e) }
                } else {
                    listOf(e)
                }
            }

            if (allExceptions.isEmpty()) {
                return
            }

            if (allExceptions.size == 1) {
                val cause = allExceptions.single()
                throw CursorException("An error occurred in a close handler. Cause: ${cause}", cause)
            } else {
                // we include the first suppressed exception in the message because on some error reporting
                // systems, the suppressed exceptions are not delivered. It's better to know at least one cause
                // rather than knowing none at all.
                throw CursorException(
                    message = "An error occurred in ${exceptions.size} close handler(s)." +
                        " Please see suppressed exceptions for details." +
                        " First suppressed exception is: ${exceptions.first()}",
                    suppressed = exceptions
                )
            }
        }

    }


    constructor() : super()

    constructor(message: String?) : super(message)

    constructor(message: String?, cause: Throwable?) : super(message, cause)

    constructor(cause: Throwable?) : super(cause)

    constructor(
        message: String?,
        cause: Throwable?,
        enableSuppression: Boolean,
        writableStackTrace: Boolean
    ) : super(message, cause, enableSuppression, writableStackTrace)

    constructor(
        message: String,
        suppressed: List<Throwable>
    ) : this(
        message = message,
        cause = null,
        enableSuppression = true,
        writableStackTrace = false
    ) {
        suppressed.forEach(::addSuppressed)
    }

}