package com.unilever.ohub.spark.domain.constraint

import com.unilever.ohub.spark.domain.DomainConstraint
import com.unilever.ohub.spark.DomainDataProvider

object CountryCodeConstraint {
  def apply(dataProvider: DomainDataProvider): CountryCodeConstraint = new CountryCodeConstraint(dataProvider)
}

class CountryCodeConstraint(val domainDataProvider: DomainDataProvider) extends DomainConstraint[String] {

  override private[domain] def isValid(value: String): Boolean = domainDataProvider.countries.get(value).isDefined

  override private[domain] def errorMessage(value: String): String = s"Country code '$value' is unknown, only known countries are supported."
}
