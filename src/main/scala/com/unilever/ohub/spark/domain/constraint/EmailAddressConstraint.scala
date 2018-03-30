package com.unilever.ohub.spark.domain.constraint

import com.unilever.ohub.spark.domain.DomainConstraint
import com.unilever.ohub.spark.generic.StringFunctions._

object EmailAddressConstraint extends DomainConstraint[String] {
  import DomainConstraint._

  override def validate(value: String): Unit =
    check(isValidEmailAddress, value, s"'$value' is not a valid email address.")
}
