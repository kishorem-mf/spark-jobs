package com.unilever.ohub.spark.tsv2parquet

import java.text.ParseException

import org.scalatest.{FunSpec, Matchers}
import CustomParsers._

class CustomParsersSpec extends FunSpec with Matchers {

  describe("parseTimeStampOption") {
    it("should parse 20171215 12:34:56") {
      assert(parseTimeStampOption("20171215 12:34:56").get.toString == "2017-12-15 12:34:56.0")
    }

    it("should parse empty string") {
      assert(parseTimeStampOption("") == None)
    }

    it("should throw exception on other input") {
      the[ParseException] thrownBy {
        parseTimeStampOption("Foo")
      } should have message "Unparseable date: \"Foo\""
    }
  }

  describe("parseDateOption") {
    it("should parse empty string") {
      assert(parseDateOption("") == None)
    }

    it("should parse 0 as None") {
      assert(parseDateOption("") == None)
    }

    it("should parse 20171215 as 2017-12-15") {
      assert(parseDateOption("20171215").get.toString == "2017-12-15")
    }

    it("should throw exception on other input") {
      the[ParseException] thrownBy {
        parseDateOption("Foo")
      } should have message "Unparseable date: \"Foo\""
    }
  }

  describe("parseLongOption") {
    it("should parse 42") {
      assert(parseLongOption("42") == Some(42L))
    }

    it("should parse \" as None") {
      assert(parseLongOption("\"") == None)
    }

    it("should parse empty string") {
      assert(parseLongOption("") == None)
    }

    it("should throw exception on other input") {
      the[NumberFormatException] thrownBy {
        parseLongOption("Foo")
      } should have message "For input string: \"Foo\""
    }
  }

  describe("parseLongRangeOption") {
    it("should parse 42") {
      assert(parseLongRangeOption("42") == Some(42L))
    }
    it("should parse 10-30") {
      assert(parseLongRangeOption("10-30") == Some(20L))
    }
    it("should parse empty string") {
      assert(parseLongRangeOption("") == None)
    }
    it("should parse other input as None") {
      assert(parseLongRangeOption("Foo") == None)
    }
  }

  describe("parseBoolOption") {
    it("should parse Y to true") {
      assert(parseBoolOption("Y") == Some(true))
    }

    it("should parse N to false") {
      assert(parseBoolOption("N") == Some(false))
    }

    it("should parse A to true") {
      assert(parseBoolOption("A") == Some(true))
    }

    it("should parse D to false") {
      assert(parseBoolOption("D") == Some(false))
    }

    it("should parse empty string to None") {
      assert(parseBoolOption("") == None)
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
