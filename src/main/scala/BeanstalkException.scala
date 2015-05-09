package org.beanstalkc

class BeanstalkException private(ex: Exception) extends Exception(ex) {
    def this(message: String) = this(new Exception(message))
    def this(message: String, cause: Throwable) = this(new Exception(message, cause))
}

object BeanstalkException {
    def apply(message: String) = new BeanstalkException(message)
    def apply(message: String, cause: Throwable) = new BeanstalkException(message, cause)
}