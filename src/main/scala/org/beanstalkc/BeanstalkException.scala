package org.beanstalkc

case class BeanstalkException(message: String) extends Exception(message)

case class BeanstalkTimeoutException(message: String) extends Exception(message)

case class BeanstalkDisconnectedException(message: String, cause: Throwable) extends Exception(message, cause)

case class BeanstalkNotFoundException(message: String) extends Exception(message)