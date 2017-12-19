package com.unilever.ohub.spark.tsv2parquet

import java.math.BigInteger
import java.text.ParseException

import org.scalatest.{FunSpec, Matchers}
import CustomParsers._

class CustomParsersSpec extends FunSpec with Matchers {

  describe("parseStringOption") {
    it("should parse an empty string") {
      assert(parseStringOption("").isEmpty)
    }

    it("should parse myTest as a string") {
      assert(parseStringOption("myTest").get.toString == "myTest")
    }
  }

  describe("parseDateTimeStampOption") {
    it("should parse empty string") {
      assert(parseDateTimeStampOption("").isEmpty)
    }

    it("should parse 0 as None") {
      assert(parseDateTimeStampOption("0").isEmpty)
    }

    it("should parse 20171215 as 2017-12-15 00:00:00") {
      assert(parseDateTimeStampOption("20171215").get.toString == "2017-12-15 00:00:00.0")
    }

    it("should parse 20171215 12:13:14 as 2017-12-15 12:13:14") {
      assert(parseDateTimeStampOption("20171215 12:13:14").get.toString == "2017-12-15 12:13:14.0")
    }

    it("should parse 2017-12-15 12:13:14 as 2017-12-15 12:13:14") {
      assert(parseDateTimeStampOption("2017-12-15 12:13:14").get.toString == "2017-12-15 12:13:14.0")
    }

    it("should parse 2017/12/15 12:13:14 as 2017-12-15 12:13:14") {
      assert(parseDateTimeStampOption("2017/12/15 12:13:14").get.toString == "2017-12-15 12:13:14.0")
    }

    it("should parse 2017.12.15 12:13:14 as 2017-12-15 12:13:14") {
      assert(parseDateTimeStampOption("2017.12.15 12:13:14").get.toString == "2017-12-15 12:13:14.0")
    }

    it("should throw exception on other input") {
      the[MatchError] thrownBy {
        parseDateTimeStampOption("Foo")
      } should have message "Foo (of class java.lang.String)"
    }
  }

  describe("parseBigDecimalOption") {
    it("should parse an empty string") {
      assert(parseBigDecimalOption("").isEmpty)
    }
    it("should parse -1000,96 as -1000.96") {
      assert(parseBigDecimalOption("-1000,96").get.toString == "-1000.96")
    }
    it("should parse -1000.96 as -1000.96") {
      assert(parseBigDecimalOption("-1000.96").get.toString == "-1000.96")
    }
    it("should parse abc as 0") {
      assert(parseBigDecimalOption("abc").get.toString == "0")
    }
  }

  describe("parseLongRangeOption") {
    it("should parse 42") {
      assert(parseLongRangeOption("42").contains(42L))
    }
    it("should parse 10-30") {
      assert(parseLongRangeOption("10-30").contains(20L))
    }
    it("should parse \" as None") {
      assert(parseLongRangeOption("\"").isEmpty)
    }
    it("should parse empty string") {
      assert(parseLongRangeOption("").isEmpty)
    }
    it("should parse other input as None") {
      assert(parseLongRangeOption("Foo").isEmpty)
    }
  }

  describe("parseBigDecimalRangeOption") {
    it("should parse None on empty string") {
      assert(parseBigDecimalRangeOption("").isEmpty)
    }
    it("should parse 42") {
      assert(parseBigDecimalRangeOption("42").contains(BigDecimal(42)))
    }
    it("should parse €42") {
      assert(parseBigDecimalRangeOption("€42").contains(BigDecimal(42)))
    }
    it("should parse 12.34") {
      assert(parseBigDecimalRangeOption("12.34").contains(BigDecimal(12.34)))
    }
    it("should parse 10.0-30.40") {
      assert(parseBigDecimalRangeOption("10.0-30.40").contains(BigDecimal(20.2)))
    }
    it("should parse €10.0-30.40") {
      assert(parseBigDecimalRangeOption("€10.0-30.40").contains(BigDecimal(20.2)))
    }
    it("should parse 10.0-€30.40") {
      assert(parseBigDecimalRangeOption("10.0-€30.40").contains(BigDecimal(20.2)))
    }
    it("should return None on unknown input") {
      assert(parseBigDecimalRangeOption("Foo").isEmpty)
    }
  }

  describe("parseBoolOption") {
    it("should parse Y to true") {
      assert(parseBoolOption("Y").contains(true))
    }

    it("should parse N to false") {
      assert(parseBoolOption("N").contains(false))
    }

    it("should parse A to true") {
      assert(parseBoolOption("A").contains(true))
    }

    it("should parse D to false") {
      assert(parseBoolOption("D").contains(false))
    }

    it("should parse empty string to None") {
      assert(parseBoolOption("").isEmpty)
    }

    it("should throw exception on other input") {
      assert(parseBoolOption("Foo").isEmpty)
    }
  }

  describe("checkLineLength") {
    it("should do nothing when the line length matches") {
      noException should be thrownBy {
        checkLineLength(Array("1", "2"), 2)
      }
    }

    it("should throw an exception when the line length doesn't match") {
      assertThrows[IllegalArgumentException] {
        checkLineLength(Array("1", "2"), 42)
      }
    }
  }

}
