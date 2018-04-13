package com.unilever.ohub.spark.domain.constraint

import com.unilever.ohub.spark.domain.DomainConstraintViolationException
import com.unilever.ohub.spark.tsv2parquet.TestDomainDataProvider
import org.scalatest.{ Matchers, WordSpec }

class SourceNameConstraintSpec extends WordSpec with Matchers {

  "Source name constraint" should {
    "throw a DomainConstraintViolationException" when {
      "a value is an unknown source" in {
        intercept[DomainConstraintViolationException] {
          SourceNameConstraint(new TestDomainDataProvider()).validate("UNKNOWN")
        }
      }
    }

    "not throw a DomainConstraintViolationException" when {
      "a value is a known source" in {
        SourceNameConstraint(new TestDomainDataProvider()).validate("EMAKINA")
      }
    }
  }
}
