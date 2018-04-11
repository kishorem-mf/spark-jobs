package com.unilever.ohub.spark.domain.constraint

import com.unilever.ohub.spark.domain.DomainConstraint

object FiniteDiscreteSetConstraint {
  def apply[T](allowedValues: Set[T]): FiniteDiscreteSetConstraint[T] = new FiniteDiscreteSetConstraint(allowedValues)
}

class FiniteDiscreteSetConstraint[T](allowedValues: Set[T]) extends DomainConstraint[T] {
  import DomainConstraint._

  override def validate(value: T): Unit =
    check[T](isValid, value, s"'$value' is not a valid value in the finite discrete set: '$allowedValues'.")

  def isValid(value: T): Boolean = allowedValues.contains(value)
}
