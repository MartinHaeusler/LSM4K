package org.chronos.chronostore.util.unit

import com.fasterxml.jackson.annotation.JsonIgnore
import org.chronos.chronostore.impl.annotations.PersistentClass
import org.chronos.chronostore.util.bits.ByteSizeUtil

@PersistentClass(format = PersistentClass.Format.JSON, details = "Used in manifest as part of Store Configurations.")
class BinarySize(
    val value: Long,
    val unit: SizeUnit,
) : Comparable<BinarySize> {

    companion object {

        val Int.Bytes: BinarySize
            get() = BinarySize(this.toLong(), SizeUnit.BYTE)

        val Int.KiB: BinarySize
            get() = BinarySize(this.toLong(), SizeUnit.KILOBYTE)

        val Int.MiB: BinarySize
            get() = BinarySize(this.toLong(), SizeUnit.MEBIBYTE)

        val Int.GiB: BinarySize
            get() = BinarySize(this.toLong(), SizeUnit.GIBIBYTE)

        val Int.TiB: BinarySize
            get() = BinarySize(this.toLong(), SizeUnit.TEBIBYTE)

        val Long.Bytes: BinarySize
            get() = BinarySize(this, SizeUnit.BYTE)

        val Long.KiB: BinarySize
            get() = BinarySize(this, SizeUnit.KILOBYTE)

        val Long.MiB: BinarySize
            get() = BinarySize(this, SizeUnit.MEBIBYTE)

        val Long.GiB: BinarySize
            get() = BinarySize(this, SizeUnit.GIBIBYTE)

        val Long.TiB: BinarySize
            get() = BinarySize(this, SizeUnit.TEBIBYTE)

    }

    init {
        require(value >= 0){
            "Binary Size cannot accept negative values (${value})!"
        }
    }

    @JsonIgnore
    val bytes: Long = this.unit.toBytes(this.value)

    override fun compareTo(other: BinarySize): Int {
        return this.bytes.compareTo(other.bytes)
    }

    operator fun div(divisor: Long): BinarySize {
        // fall back to bytes to avoid precision loss
        return BinarySize(this.bytes / divisor, SizeUnit.BYTE)
    }

    operator fun times(factor: Long): BinarySize {
        // stay in same unit to avoid numeric overflow
        return BinarySize(this.value * factor, unit)
    }


    override fun toString(): String {
        return "${this.value} ${this.unit}"
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as BinarySize

        return bytes == other.bytes
    }

    override fun hashCode(): Int {
        return bytes.hashCode()
    }

    fun toHumanReadableString(): String {
        return ByteSizeUtil.humanReadableByteCountBin(this.bytes)
    }

}