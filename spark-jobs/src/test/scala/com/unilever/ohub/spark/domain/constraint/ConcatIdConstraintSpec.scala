package com.unilever.ohub.spark.domain.constraint

import com.unilever.ohub.spark.domain.DomainConstraintViolationException
import org.scalatest.{ Matchers, WordSpec }

class ConcatIdConstraintSpec extends WordSpec with Matchers {

  "Finite discrete set constraint" should {
    "throw a DomainConstraintViolationException" when {
      "a value does not match the concat id format" in {
        intercept[DomainConstraintViolationException] {
          ConcatIdConstraint.validate("X", "A", "B", "C")
        }
      }
    }

    "not throw a DomainConstraintViolationException" when {
      "a value matches the concat id format" in {
        ConcatIdConstraint.validate("A~B~C", "A", "B", "C")
      }
    }
  }
}
