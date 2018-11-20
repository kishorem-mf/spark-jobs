package com.unilever.ohub.spark.ingest

import java.time.format.DateTimeParseException

import org.scalatest.{ FunSpec, Matchers }
import CustomParsers._
import org.apache.log4j.{ LogManager, Logger }

class CustomParsersSpec extends FunSpec with Matchers {
  implicit val testLogger: Logger = LogManager.getLogger(this.getClass)

  describe("parseDateUnsafe") {
    it("should throw an exception for an empty string") {
      assertThrows[DateTimeParseException](parseDateUnsafe()(""))
    }

    it("should throw an exception for input '0'") {
      assertThrows[DateTimeParseException](parseDateUnsafe()("0"))
    }

    intercept[DateTimeParseException] {
      parseDateUnsafe()("some-illegal-value")
    }.getMessage shouldBe "Text 'some-illegal-value' could not be parsed at index 0"

    it("should throw an exception for input '20171215 00:00:00'") {
      assertThrows[DateTimeParseException](parseDateUnsafe()("20171215 00:00:00"))
    }

    it("should parse 20171215 as 2017-12-15 00:00:00") {
      assert(parseDateUnsafe()("20171215").toString == "2017-12-15 00:00:00.0")
    }
  }

  describe("parseDateTimeUnsafe") {
    it("should throw an exception for an empty string") {
      assertThrows[DateTimeParseException](parseDateTimeUnsafe()(""))
    }

    it("should throw an exception for input '0'") {
      assertThrows[DateTimeParseException](parseDateTimeUnsafe()("0"))
    }

    intercept[DateTimeParseException] {
      parseDateTimeUnsafe()("some-illegal-value")
    }.getMessage shouldBe "Text 'some-illegal-value' could not be parsed at index 0"

    it("should parse 20171215 12:13:14 as 2017-12-15 12:13:14") {
      assert(parseDateTimeUnsafe()("20171215 12:13:14").toString == "2017-12-15 12:13:14.0")
    }
  }

  describe("toTimestamp") {
    it("should parse a timestamp correctly from a long input string") {
      assert(toTimestamp("1542205922011").getTime == 1542205922011L)
    }
  }

  describe("toBigDecimal") {
    it("should throw an exception on an empty string") {
      assertThrows[Exception](toBigDecimal(""))
    }
    it("should parse 42") {
      assert(toBigDecimal("42") == BigDecimal(42))
    }
    it("should throw an exception on other input") {
      assertThrows[Exception](toBigDecimal("Foo"))
    }
  }

  describe("toInt") {
    it("should throw an exception on an empty string") {
      assertThrows[Exception](toInt(""))
    }
    it("should parse 42") {
      assert(toInt("42") == 42)
    }
    it("should throw an exception on other input") {
      assertThrows[Exception](toInt("Foo"))
    }
  }

  describe("toLong") {
    it("should throw an exception on an empty string") {
      assertThrows[Exception](toLong(""))
    }
    it("should parse 42") {
      assert(toLong("42") == 42L)
    }
    it("should throw an exception on other input") {
      assertThrows[Exception](toLong("Foo"))
    }
  }

  describe("toBoolean") {
    it("should parse true correctly") {
      assert(toBoolean("TRUE") == true)
    }

    it("should parse false correctly") {
      assert(toBoolean("false") == false)
    }

    it("should parse empty string to None") {
      assertThrows[Exception](toBoolean(""))
    }

    it("should throw exception on other input") {
      assertThrows[Exception](toBoolean("Foo"))
    }
  }
}
