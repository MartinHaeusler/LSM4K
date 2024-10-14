package org.chronos.chronostore.util.cursor

import org.chronos.chronostore.api.exceptions.CursorException

object CursorUtils {

    /**
     * Executes the given close handlers.
     *
     * This method safely executes all given handlers, collecting any exceptions thrown
     * in the process. Each handler is guaranteed to be executed, even if the previous
     * handler has thrown an exception. The handlers are guaranteed to be executed in
     * the given order.
     *
     * If at least one of the handlers has thrown an exception, a [CursorException]
     * will be thrown. If there is exactly one exception to be reported, it will be attached
     * as the [cause][CursorException.cause]. If there are multiple exceptions
     * (this can happen if multiple handlers have failed) then they will be included as
     * [suppressed exceptions][CursorException.suppressedExceptions]. Either way,
     * the exception message is guaranteed to include the [Throwable.toString] result of
     * the first handler exception.
     *
     * @param handlers A list of close handlers to execute in the given order.
     *
     * @throws CursorException if at least one of the handlers has failed, as described above.
     */
    fun executeCloseHandlers(vararg handlers: CloseHandler?) {
        executeCloseHandlers(closeInternal = null, closeHandlers = handlers.filterNotNull())
    }

    /**
     * Executes the given close handlers.
     *
     * This method safely executes all given handlers, collecting any exceptions thrown
     * in the process. Each handler is guaranteed to be executed, even if the previous
     * handler has thrown an exception. The handlers are guaranteed to be executed in
     * the given order.
     *
     * If at least one of the handlers has thrown an exception, a [CursorException]
     * will be thrown. If there is exactly one exception to be reported, it will be attached
     * as the [cause][CursorException.cause]. If there are multiple exceptions
     * (this can happen if multiple handlers have failed) then they will be included as
     * [suppressed exceptions][CursorException.suppressedExceptions]. Either way,
     * the exception message is guaranteed to include the [Throwable.toString] result of
     * the first handler exception.
     *
     * @param handlers A list of close handlers to execute in the given order.
     *
     * @throws CursorException if at least one of the handlers has failed, as described above.
     */
    fun executeCloseHandlers(handlers: List<CloseHandler>) {
        executeCloseHandlers(closeInternal = null, closeHandlers = handlers)
    }

    /**
     * Executes the given close handlers.
     *
     * This method safely executes all given handlers, collecting any exceptions thrown
     * in the process. Each handler is guaranteed to be executed, even if the previous
     * handler has thrown an exception. The handlers are guaranteed to be executed in
     * the given order.
     *
     * If at least one of the handlers has thrown an exception, a [CursorException]
     * will be thrown. If there is exactly one exception to be reported, it will be attached
     * as the [cause][CursorException.cause]. If there are multiple exceptions
     * (this can happen if multiple handlers have failed) then they will be included as
     * [suppressed exceptions][CursorException.suppressedExceptions]. Either way,
     * the exception message is guaranteed to include the [Throwable.toString] result of
     * the first handler exception.
     *
     * @param closeHandlers A list of close handlers to execute in the given order.
     * @param closeInternal An additional close listener which is not part of the list (if any).
     *                      Behaves as if it was added to the beginning of the list; this parameter only exists
     *                      to avoid creating another list.
     *
     * @throws CursorException if at least one of the handlers has failed, as described above.
     */
    fun executeCloseHandlers(closeInternal: CloseHandler? = null, closeHandlers: List<CloseHandler>) {
        // by default, we assume that no exceptions will happen, so we defer the list
        // creation until we find the first exception.
        var exceptions: MutableList<Throwable>? = null
        if (closeInternal != null) {
            try {
                closeInternal.invoke()
            } catch (t: Throwable) {
                // we may have to initialize the list of errors
                val list = if (exceptions != null) {
                    exceptions
                } else {
                    exceptions = mutableListOf()
                    exceptions
                }
                list += t
            }
        }
        for (closeHandler in closeHandlers) {
            try {
                closeHandler.invoke()
            } catch (t: Throwable) {
                // we may have to initialize the list of errors
                val list = if (exceptions != null) {
                    exceptions
                } else {
                    exceptions = mutableListOf()
                    exceptions
                }
                list += t
            }
        }

        CursorException.throwIfExceptionsArePresent(exceptions)
    }

}