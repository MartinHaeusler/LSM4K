package org.lsm4k.impl.annotations

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
