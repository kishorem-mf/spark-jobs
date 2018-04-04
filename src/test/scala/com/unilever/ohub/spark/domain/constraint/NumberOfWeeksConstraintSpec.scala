package com.unilever.ohub.spark.domain.constraint

import com.unilever.ohub.spark.domain.DomainConstraintViolationException
import org.scalatest.{Matchers, WordSpec}

class NumberOfWeeksConstraintSpec extends WordSpec with Matchers {

  "Number of weeks constraint" should {
    "throw a DomainConstraintViolationException" when {
      "a too small value is provided" in {
        intercept[DomainConstraintViolationException] {
          NumberOfWeeksConstraint.validate(-1)
        }
      }

      "a too large value is provided" in {
        intercept[DomainConstraintViolationException] {
          NumberOfWeeksConstraint.validate(53)
        }
      }
    }

    "not throw a DomainConstraintViolationException" when {
      "the lower bound is provided" in {
        NumberOfWeeksConstraint.validate(0)
      }

      "the upper bound is provided" in {
        NumberOfWeeksConstraint.validate(52)
      }
    }
  }
}
