package org.beanstalkc

case class BeanstalkException(message: String) extends Exception(message)

case class BeanstalkTimeoutException(message: String) extends Exception(message)

case class BeanstalkDisconnectedException(message: String) extends Exception(message)

case class BeanstalkJobNotFoundException(message: String) extends Exception(message)