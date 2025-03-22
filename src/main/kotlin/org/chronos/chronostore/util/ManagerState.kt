package org.chronos.chronostore.util

enum class ManagerState {

    OPEN {

        override fun checkOpen() {
            // no-op, it's open
        }

        override fun isClosed(): Boolean {
            return false
        }

    },

    CLOSED {

        override fun checkOpen() {
            throw IllegalStateException("This Database has already been closed.")
        }

        override fun isClosed(): Boolean {
            return true
        }

    },

    PANIC {

        override fun checkOpen() {
            throw IllegalStateException("The Database has been closed due to a fatal error. Please check the error logs.")
        }

        override fun isClosed(): Boolean {
            return true
        }

    },

    ;

    abstract fun checkOpen()

    abstract fun isClosed(): Boolean

}