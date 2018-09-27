package com.unilever.ohub.spark.domain.constraint

import com.unilever.ohub.spark.domain.DomainConstraint

object NumberOfDaysConstraint extends DomainConstraint[Int] {

  override def isValid(numberOfDays: Int): Boolean =
    numberOfDays >= 0 && numberOfDays <= 7

  override def errorMessage(value: Int): String =
    s"'$value' is not a valid number of days, should be in [0, 7]."
}
