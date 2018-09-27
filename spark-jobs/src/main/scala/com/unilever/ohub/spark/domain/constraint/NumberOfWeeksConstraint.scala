package com.unilever.ohub.spark.domain.constraint

import com.unilever.ohub.spark.domain.DomainConstraint

object NumberOfWeeksConstraint extends DomainConstraint[Int] {

  def isValid(numberOfWeeks: Int): Boolean =
    numberOfWeeks >= 0 && numberOfWeeks <= 52

  def errorMessage(value: Int): String =
    s"'$value' is not a valid number of weeks, should be in [0, 52]."
}
