package com.unilever.ohub.spark.tsv2parquet

import java.text.ParseException

import org.scalatest.{FunSpec, Matchers}
import CustomParsers._

class CustomParsersSpec extends FunSpec with Matchers {

  describe("parseDateTimeStampOption") {
    it("should parse empty string") {
      assert(parseDateTimeStampOption("").isEmpty)
    }

    it("should parse 0 as None") {
      assert(parseDateTimeStampOption("0").isEmpty)
    }

    it("should parse 20171215 as 2017-12-15 00:00:00") {
      assert(parseDateTimeStampOption("20171215").get.toString == "2017-12-15 00:00:00")
    }

    it("should parse 20171215 12:13:14 as 2017-12-15 12:13:14") {
      assert(parseDateTimeStampOption("20171215 12:13:14").get.toString == "2017-12-15 12:13:14")
    }

    it("should parse 2017-12-15 12:13:14 as 2017-12-15 12:13:14") {
      assert(parseDateTimeStampOption("2017-12-15 12:13:14").get.toString == "2017-12-15 12:13:14")
    }

    it("should parse 2017/12/15 12:13:14 as 2017-12-15 12:13:14") {
      assert(parseDateTimeStampOption("2017/12/15 12:13:14").get.toString == "2017-12-15 12:13:14")
    }

    it("should parse 2017.12.15 12:13:14 as 2017-12-15 12:13:14") {
      assert(parseDateTimeStampOption("2017.12.15 12:13:14").get.toString == "2017-12-15 12:13:14")
    }

    it("should throw exception on other input") {
      the[ParseException] thrownBy {
        parseDateTimeStampOption("Foo")
      } should have message "Unparseable date: \"Foo\""
    }
  }

  describe("parseLongRangeOption") {
    it("should parse 42") {
      assert(parseLongRangeOption("42").contains(42L))
    }

    it("should parse \" as None") {
      assert(parseLongRangeOption("\"").isEmpty)
    }

    it("should parse empty string") {
      assert(parseLongRangeOption("").isEmpty)
    }

    it("should throw exception on other input") {
      the[NumberFormatException] thrownBy {
        parseLongRangeOption("Foo")
      } should have message "For input string: \"Foo\""
    }
  }

  describe("parseLongRangeOption") {
    it("should parse 42") {
      assert(parseLongRangeOption("42").contains(42L))
    }
    it("should parse 10-30") {
      assert(parseLongRangeOption("10-30").contains(20L))
    }
    it("should parse empty string") {
      assert(parseLongRangeOption("").isEmpty)
    }
    it("should parse other input as None") {
      assert(parseLongRangeOption("Foo").isEmpty)
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
      the[IllegalArgumentException] thrownBy {
        parseBoolOption("Foo")
      } should have message "Unsupported boolean value: Foo"
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
