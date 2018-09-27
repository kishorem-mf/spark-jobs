package com.unilever.ohub.spark.domain.constraint

import com.unilever.ohub.spark.domain.DomainConstraint

object FiniteDiscreteSetConstraint {
  def apply[T](constraintName: String, allowedValues: Set[T]): FiniteDiscreteSetConstraint[T] =
    new FiniteDiscreteSetConstraint(constraintName, allowedValues)
}

class FiniteDiscreteSetConstraint[T](constraintName: String, allowedValues: Set[T]) extends DomainConstraint[T] {

  override def isValid(value: T): Boolean = allowedValues.contains(value)

  override def errorMessage(value: T): String =
    s"constraint '$constraintName' on '$value' is not a valid value in the finite discrete set: '$allowedValues'."
}
