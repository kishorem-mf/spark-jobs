package com.unilever.ohub.spark.domain.constraint

import com.unilever.ohub.spark.domain.DomainConstraint
import com.unilever.ohub.spark.generic.StringFunctions._

object EmailAddressConstraint extends DomainConstraint[String] {

  override def isValid(value: String): Boolean = isValidEmailAddress(value)

  override def errorMessage(value: String): String = s"'$value' is not a valid email address."
}
