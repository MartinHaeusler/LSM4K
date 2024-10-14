package org.chronos.chronostore.impl.annotations

@MustBeDocumented
@Target(AnnotationTarget.CLASS)
annotation class PersistentClass(
    val format: Format,
    val details: String = "",
) {

    enum class Format {
        JSON,
        BINARY,
    }

}
