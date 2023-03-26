package org.example.dbfromzero.io.vfs

import java.io.InputStream

interface InputSource {

    fun createInputStream(): InputStream

}