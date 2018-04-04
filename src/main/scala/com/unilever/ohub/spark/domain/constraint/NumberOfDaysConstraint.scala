package com.unilever.ohub.spark.domain.constraint

import com.unilever.ohub.spark.domain.DomainConstraint

object NumberOfDaysConstraint extends DomainConstraint[Int] {
  import DomainConstraint._

  override def validate(value: Int): Unit =
    check[Int](isValidDays, value, s"'$value' is not a valid number of days, should be in [0, 7].")

  def isValidDays(numberOfDays: Int): Boolean = numberOfDays >= 0 && numberOfDays <=7
}
