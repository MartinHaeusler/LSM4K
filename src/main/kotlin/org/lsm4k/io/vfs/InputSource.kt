package org.lsm4k.io.vfs

import java.io.InputStream

interface InputSource {

    fun createInputStream(): InputStream

}