package com.unilever.ohub.spark.domain.constraint

import com.unilever.ohub.spark.domain.DomainConstraintViolationException
import org.scalatest.{ Matchers, WordSpec }

class EmailAddressConstraintSpec extends WordSpec with Matchers {

  "Email address constraint" should {
    "throw a DomainConstraintViolationException" when {
      "an invalid email address is provided" in {
        intercept[DomainConstraintViolationException] {
          EmailAddressConstraint.validate("some-string-value")
        }
        intercept[DomainConstraintViolationException] {
          EmailAddressConstraint.validate("some-string-value@")
        }
        intercept[DomainConstraintViolationException] {
          EmailAddressConstraint.validate("some-string-value@something")
        }
      }
    }

    "not throw a DomainConstraintViolationException" when {
      "a valid email address is provided" in {
        EmailAddressConstraint.validate("my-email-address@some-server.com")
        EmailAddressConstraint.validate("my.extra.long.email.address@some.long.distance.server.com")
      }
    }
  }
}
