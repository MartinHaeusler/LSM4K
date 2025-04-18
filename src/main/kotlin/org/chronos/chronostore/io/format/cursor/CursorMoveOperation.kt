package org.chronos.chronostore.io.format.cursor

enum class CursorMoveOperation {

    NEXT {

        override val isRandomJump: Boolean
            get() = false
    },

    PREVIOUS {

        override val isRandomJump: Boolean
            get() = false

    },

    FIRST {

        override val isRandomJump: Boolean
            get() = true

    },

    LAST {

        override val isRandomJump: Boolean
            get() = true

    },

    SEEK_NEXT {

        override val isRandomJump: Boolean
            get() = true

    },

    SEEK_PREVIOUS {

        override val isRandomJump: Boolean
            get() = true

    },

    ;


    abstract val isRandomJump: Boolean

}