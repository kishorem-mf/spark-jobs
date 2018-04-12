package com.unilever.ohub.spark.domain.constraint

import com.unilever.ohub.spark.domain.DomainConstraint

object FiniteDiscreteSetConstraint {
  def apply[T](allowedValues: Set[T]): FiniteDiscreteSetConstraint[T] = new FiniteDiscreteSetConstraint(allowedValues)
}

class FiniteDiscreteSetConstraint[T](allowedValues: Set[T]) extends DomainConstraint[T] {

  override def isValid(value: T): Boolean = allowedValues.contains(value)

  override def errorMessage(value: T): String =
    s"'$value' is not a valid value in the finite discrete set: '$allowedValues'."
}
