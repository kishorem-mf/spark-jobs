package com.unilever.ohub.spark.domain.constraint

import com.unilever.ohub.spark.domain.DomainConstraintViolationException
import com.unilever.ohub.spark.ingest.TestDomainDataProvider
import org.scalatest.{ Matchers, WordSpec }

class SourceNameConstraintSpec extends WordSpec with Matchers {

  "Source name constraint" should {
    "throw a DomainConstraintViolationException" when {
      "a value is an unknown source" in {
        intercept[DomainConstraintViolationException] {
          SourceNameConstraint(TestDomainDataProvider()).validate("UNKNOWN")
        }
      }
    }

    "not throw a DomainConstraintViolationException" when {
      "a value is a known source" in {
        SourceNameConstraint(TestDomainDataProvider()).validate("EMAKINA")
      }
    }
  }
}
