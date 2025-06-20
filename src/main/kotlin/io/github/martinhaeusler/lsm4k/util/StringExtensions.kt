package io.github.martinhaeusler.lsm4k.util

object StringExtensions {

    private val NEWLINE_REGEX = """\r?\n""".toRegex()

    fun String.ellipsis(maxLength: Int, continuationSymbol: String = "..."): String {
        require(maxLength > 0) { "Argument 'maxLength' must be positive, but got ${maxLength}!" }
        require(continuationSymbol.isNotEmpty()) { "Argument 'continuationSymbol' must not be empty!" }
        require(continuationSymbol.length < maxLength) { "Argument 'continuationSymbol' has too many characters for maxLength ${maxLength}!" }
        if (this.length <= maxLength) {
            return this
        }
        return this.substring(0, maxLength - continuationSymbol.length) + continuationSymbol
    }

    fun String.toSingleLine(): String {
        return this.replace(NEWLINE_REGEX, " ")
    }

}