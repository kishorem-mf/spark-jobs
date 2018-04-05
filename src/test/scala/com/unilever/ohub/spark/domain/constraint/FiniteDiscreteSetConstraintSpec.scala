package com.unilever.ohub.spark.domain.constraint

import com.unilever.ohub.spark.domain.DomainConstraintViolationException
import org.scalatest.{ Matchers, WordSpec }

class FiniteDiscreteSetConstraintSpec extends WordSpec with Matchers {

  val constraint = FiniteDiscreteSetConstraint(Set("A", "B", "C"))

  "Finite discrete set constraint" should {
    "throw a DomainConstraintViolationException" when {
      "a value is not contained in the set" in {
        intercept[DomainConstraintViolationException] {
          constraint.validate("X")
        }
      }
    }

    "not throw a DomainConstraintViolationException" when {
      "a value is contained in the set" in {
        constraint.validate("B")
      }
    }
  }
}
