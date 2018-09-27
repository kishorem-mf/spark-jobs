package com.unilever.ohub.spark.domain.constraint

import com.unilever.ohub.spark.domain.DomainConstraintViolationException
import org.scalatest.{ Matchers, WordSpec }

class NumberOfDaysConstraintSpec extends WordSpec with Matchers {

  "Number of days constraint" should {
    "throw a DomainConstraintViolationException" when {
      "a too small value is provided" in {
        intercept[DomainConstraintViolationException] {
          NumberOfDaysConstraint.validate(-1)
        }
      }

      "a too large value is provided" in {
        intercept[DomainConstraintViolationException] {
          NumberOfDaysConstraint.validate(8)
        }
      }
    }

    "not throw a DomainConstraintViolationException" when {
      "the lower bound is provided" in {
        NumberOfDaysConstraint.validate(0)
      }

      "the upper bound is provided" in {
        NumberOfDaysConstraint.validate(7)
      }
    }
  }
}
