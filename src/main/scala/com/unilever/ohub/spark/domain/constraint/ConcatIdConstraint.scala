package com.unilever.ohub.spark.domain.constraint

import com.unilever.ohub.spark.domain.DomainConstraint
import ConcatIdConstraintTypes._

object ConcatIdConstraintTypes {
  type ConcatId = String
  type CountryCode = String
  type SourceName = String
  type SourceEntityId = String
}

object ConcatIdConstraint extends DomainConstraint[(ConcatId, CountryCode, SourceName, SourceEntityId)] {
  import DomainConstraint._

  override def validate(value: (ConcatId, CountryCode, SourceName, SourceEntityId)): Unit =
    check[(ConcatId, CountryCode, SourceName, SourceEntityId)](isValid, value,
      s"'${value._1}' is not a valid concatId, it should follow the following format '<country-code>~<source-name>~<source-entity-id>', where <country-code> is '${value._2}', <source-name> is'${value._3}' and <source-entity-id> is '${value._4}'"
    )

  def isValid(value: (ConcatId, CountryCode, SourceName, SourceEntityId)): Boolean = {
    val (concatId, countryCode, sourceName, sourceEntityId) = value
    val expectedConcatId = s"$countryCode~$sourceName~$sourceEntityId"

    concatId == expectedConcatId
  }
}
