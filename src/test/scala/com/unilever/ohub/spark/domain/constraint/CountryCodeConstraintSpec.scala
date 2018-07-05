package com.unilever.ohub.spark.domain.constraint

import com.unilever.ohub.spark.domain.DomainConstraintViolationException
import com.unilever.ohub.spark.ingest.TestDomainDataProvider
import org.scalatest.{ Matchers, WordSpec }

class CountryCodeConstraintSpec extends WordSpec with Matchers {

  "Country code constraint" should {
    "throw a DomainConstraintViolationException" when {
      "a value is an unknown country" in {
        intercept[DomainConstraintViolationException] {
          CountryCodeConstraint(TestDomainDataProvider()).validate("UNKNOWN")
        }
      }
    }

    "not throw a DomainConstraintViolationException" when {
      "a value is a known country" in {
        CountryCodeConstraint(TestDomainDataProvider()).validate("NL")
      }
    }
  }
}
