package io.github.martinhaeusler.lsm4k.util

enum class OperatingSystem {

    WINDOWS, LINUX, MAC, SOLARIS;

    companion object {

        val current: OperatingSystem? by lazy {
            val operSys = System.getProperty("os.name").lowercase()
            when {
                operSys.contains("win") -> WINDOWS
                operSys.contains("nix") || operSys.contains("nux") || operSys.contains("aix") -> LINUX
                operSys.contains("mac") -> MAC
                operSys.contains("sunos") -> SOLARIS
                else -> null
            }
        }

        val isLinux: Boolean by lazy {
            this.current == LINUX
        }

        val isWindows: Boolean by lazy {
            this.current == WINDOWS
        }

        val isMacOS: Boolean by lazy {
            this.current == MAC
        }

    }

}
