package com.unilever.ohub.spark.generic

import org.scalatest.{ FunSpec, Matchers }

class StringFunctionsTest extends FunSpec with Matchers {
  describe("createConcatId") {
    describe("when given an empty country code") {
      val countryCode = Option.empty[String]

      describe("and given an empty source") {
        val source = Option.empty[String]

        describe("and given an empty refId") {
          val refId = Option.empty[String]

          it("should produce the correct concatenated ID") {
            StringFunctions.createConcatId(countryCode, source, refId) shouldEqual "~~"
          }
        }

        describe("and given a filled refId") {
          val refId = Some("ReferenceID")

          it("should produce the correct concatenated ID") {
            StringFunctions.createConcatId(countryCode, source, refId) shouldEqual "~~ReferenceID"
          }
        }
      }

      describe("and given a filled source") {
        val source = Some("Sauce")

        describe("and given an empty refId") {
          val refId = Option.empty[String]

          it("should produce the correct concatenated ID") {
            StringFunctions.createConcatId(countryCode, source, refId) shouldEqual "~Sauce~"
          }
        }

        describe("and given a filled refId") {
          val refId = Some("ReferenceID")

          it("should produce the correct concatenated ID") {
            StringFunctions.createConcatId(countryCode, source, refId) shouldEqual "~Sauce~ReferenceID"
          }
        }
      }
    }

    describe("when given a filled country code") {
      val countryCode = Some("NL")

      describe("and given an empty source") {
        val source = Option.empty[String]

        describe("and given an empty refId") {
          val refId = Option.empty[String]

          it("should produce the correct concatenated ID") {
            StringFunctions.createConcatId(countryCode, source, refId) shouldEqual "NL~~"
          }
        }

        describe("and given a filled refId") {
          val refId = "ReferenceID"

          it("should produce the correct concatenated ID") {
            StringFunctions.createConcatId(countryCode, source, refId) shouldEqual "NL~~ReferenceID"
          }
        }
      }

      describe("and given a filled source") {
        val source = Some("Sauce")

        describe("and given an empty refId") {
          val refId = Option.empty[String]

          it("should produce the correct concatenated ID") {
            StringFunctions.createConcatId(countryCode, source, refId) shouldEqual "NL~Sauce~"
          }
        }

        describe("and given a filled refId") {
          val refId = Some("ReferenceID")

          it("should produce the correct concatenated ID") {
            StringFunctions.createConcatId(countryCode, source, refId) shouldEqual "NL~Sauce~ReferenceID"
          }
        }
      }
    }
  }
}
