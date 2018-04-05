package com.unilever.ohub.spark.domain.constraint

import com.unilever.ohub.spark.domain.DomainConstraint
import com.unilever.ohub.spark.domain.DomainConstraint.check

object NumberOfWeeksConstraint extends DomainConstraint[Int] {

  override def validate(value: Int): Unit =
    check[Int](isValidWeeks, value, s"'$value' is not a valid number of weeks, should be in [0, 52].")

  def isValidWeeks(numberOfWeeks: Int): Boolean = numberOfWeeks >= 0 && numberOfWeeks <= 52
}
