package com.unilever.ohub.spark.domain

object DomainConstraintViolationException {
  def apply(errorMessage: String): DomainConstraintViolationException = new DomainConstraintViolationException(errorMessage)
}

class DomainConstraintViolationException(message: String) extends IllegalArgumentException(message)

object DomainConstraint {

  @throws(classOf[DomainConstraintViolationException])
  def check[T](isValid: T ⇒ Boolean, value: T, errorMessage: String): Unit =
    if (!isValid(value)) throw DomainConstraintViolationException(errorMessage)
}

trait DomainConstraint[T] {

  @throws(classOf[DomainConstraintViolationException])
  def validate(value: T): Unit
}
