package org.beanstalkc

import java.io.BufferedInputStream
import java.net.Socket

import scala.collection.mutable.ArrayBuffer

class SocketConnect(host: String, port: Int) extends BeanstalkConnect {
    private val conn = new Socket(host, port)
    private val inputStream = new BufferedInputStream(conn.getInputStream)
    private val outputStream = conn.getOutputStream

    def this() {
        this(BeanstalkConnect.DEFAULT_HOST, BeanstalkConnect.DEFAULT_PORT)
    }

    def this(host: String) {
        this(host, BeanstalkConnect.DEFAULT_PORT)
    }

    override def write(command: String): Unit = {
        val data = command + BeanstalkConnect.CRLF
        outputStream.write(data.getBytes)
        outputStream.flush()
    }

    override def write(command: String, data: Array[Byte]): Unit = {
        val com = command + BeanstalkConnect.CRLF
        outputStream.write(com.getBytes)
        outputStream.write(data)
        outputStream.write(BeanstalkConnect.CRLF.getBytes)
        outputStream.flush()
    }

    override def getBody(size: Int): Array[Byte] = {
        val buffer = new ArrayBuffer[Byte]
        for(i <- 0 until size) {
            val char = inputStream.read()
            buffer += char.toByte
        }

        // read CRLF
        inputStream.read()
        inputStream.read()

        buffer.toArray
    }

    override def getResponse(): List[String] = {
        val buffer = new StringBuilder()
        do {
            val cur = inputStream.read().toChar
            buffer += cur
        }while(!buffer.endsWith(BeanstalkConnect.CRLF))

        buffer.dropRight(2).toString().split(" ").toList
    }

    override def close(): Unit = {
        conn.close()
    }
}