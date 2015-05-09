package org.beanstalkc

trait BeanstalkConnect {
    protected def write(command: String)
    protected def write(command: String, data: Array[Byte])
    protected def getResponse(): List[String]
    def getBody(size: Int): Array[Byte]
    def close(): Unit

    def sendCommand(command: String): Unit = {
        write(command)
    }

    def sendCommand(command: String, expect: String, expectErr: List[String]): List[String] = {
        write(command)
        handleResponse(command, expect, expectErr)
    }

    def sendCommand(command: String, data: Array[Byte], expect: String, expectErr: List[String]): List[String] = {
        write(command, data)
        handleResponse(command, expect, expectErr)
    }

    private def handleResponse(command: String, expect: String, expectErr: List[String]): List[String] = {
        val response = getResponse()
        if(expect.equals(response(0))) {
            return response.drop(1)
        }
        else if(expectErr.contains(response(0))) {
            throw BeanstalkException(response.mkString(" "))
        }
        else {
            val message = String.format("%s unexpected response: %s", command, response.mkString(" "))
            throw new RuntimeException(message)
        }
    }
}

object BeanstalkConnect {
    val DEFAULT_PORT = 11300
    val DEFAULT_HOST = "localhost"
    val CRLF = "\r\n"
}
