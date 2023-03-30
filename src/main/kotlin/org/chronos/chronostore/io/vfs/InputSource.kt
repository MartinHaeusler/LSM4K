package org.chronos.chronostore.io.vfs

import java.io.InputStream

interface InputSource {

    fun createInputStream(): InputStream

}