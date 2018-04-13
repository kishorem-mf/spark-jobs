package com.unilever.ohub.spark.domain

object DomainConstraintViolationException {
  def apply(errorMessage: String): DomainConstraintViolationException = new DomainConstraintViolationException(errorMessage)
}

class DomainConstraintViolationException(message: String) extends IllegalArgumentException(message)

trait DomainConstraint[T] {

  private[domain] def isValid(value: T): Boolean

  private[domain] def errorMessage(value: T): String

  @throws(classOf[DomainConstraintViolationException])
  def validate(value: T): Unit =
    if (!isValid(value)) throw DomainConstraintViolationException(errorMessage(value))
}
