package com.unilever.ohub.spark.domain.constraint

import com.unilever.ohub.spark.domain.DomainConstraint
import com.unilever.ohub.spark.DomainDataProvider

object SourceNameConstraint {
  def apply(dataProvider: DomainDataProvider): SourceNameConstraint = new SourceNameConstraint(dataProvider)
}

class SourceNameConstraint(val domainDataProvider: DomainDataProvider) extends DomainConstraint[String] {

  override private[domain] def isValid(value: String): Boolean = domainDataProvider.sourcePreferences.get(value.toUpperCase).isDefined

  override private[domain] def errorMessage(value: String): String = s"Source name '$value' is unknown, only known sources are supported."
}
