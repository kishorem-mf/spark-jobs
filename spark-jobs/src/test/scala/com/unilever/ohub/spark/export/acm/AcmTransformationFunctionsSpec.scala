package com.unilever.ohub.spark.export.acm

import java.sql.Timestamp
import java.time.LocalDateTime

import org.scalatest.FunSpec

class AcmTransformationFunctionsSpec extends FunSpec with AcmTransformationFunctions  {

  var acmFunctions = new AcmTransformationFunctions {}

  describe("When you have a true boolean value") {
    it("should convert to 1") {
      assert(acmFunctions.booleanTo10Converter(true) == "1")
    }
    it("should convert to Y") {
      assert(acmFunctions.booleanToYNConverter(true) == "Y")
    }
    it("should convert to Y with unknown converter") {

      assert((Some(true).booleanToYNU) == "Y")
    }
  }

  describe("When you have a false boolean value") {
    it("should convert to 0") {
      assert(acmFunctions.booleanTo10Converter(false) == "0")
    }
    it("should convert to N") {
      assert(acmFunctions.booleanToYNConverter(false) == "N")
    }
    it("should convert to N with unknown converter") {
      assert(Some(false).booleanToYNU == "N")
    }
  }

  describe("When you have an undefined boolean value") {
    it("should convert to U") {
      val value: Option[Boolean] = Option.empty
      assert(value.booleanToYNU == "U")
    }
  }

  describe("When string contains all weird character") {
    it("should be an empty string after cleanup") {
      assert(acmFunctions.cleanString("\u0021\u0023\u0025\u0026\u0028\u0029\u002A\u002B\u002D\u002F\u003A\u003B\u003C\u003D\u003E\u003F\u0040\u005E\u007C\u007E\u00A8\u00A9\u00AA\u00AC\u00AD\u00AF\u00B0\u00B1\u00B2\u00B3\u00B6\u00B8\u00B9\u00BA\u00BB\u00BC\u00BD\u00BE\u2013\u2014\u2022\u2026\u20AC\u2121\u2122\u2196\u2197\u247F\u250A\u2543\u2605\u2606\u3001\u3002\u300C\u300D\u300E\u300F\u3010\u3011\uFE36\uFF01\uFF06\uFF08\uFF09\uFF1A\uFF1B\uFF1F\u007B\u007D\u00AE\u00F7\u1BFC\u1BFD\u2260\u2264\u2DE2\u2DF2\uEC66\uEC7C\uEC7E\uED2B\uED34\uED3A\uEDAB\uEDFC\uEE3B\uEEA3\uEF61\uEFA2\uEFB0\uEFB5\uEFEA\uEFED\uFDAB\uFFB7\u007F\u24D2\u2560\u2623\u263A\u2661\u2665\u266A\u2764\uE2B1\uFF0D˱˳˵˶˹˻˼˽") == "")
    }
  }

  describe("When string contains a weird character") {
    it("should only remove that character") {
      assert(acmFunctions.cleanString("test˵˶˹˻˼˽") == "test")
    }
  }

  describe("When string contains a dash character") {
    it("should remove that character") {
      assert(acmFunctions.cleanString("jean-pierre") == "jeanpierre")
    }
  }

  describe("When string only contains a dot") {
    it("should remove that character and return empty string") {
      assert(acmFunctions.cleanString(".") == "")
    }
  }

  describe("When converting timestamp") {
    it("should convert to ACM wished format") {
      val ts = LocalDateTime.of(2019, 11, 12, 10, 11, 12)
      assert(acmFunctions.timestampToString(Timestamp.valueOf(ts)) == "2019/11/12 10:11:12")
    }
  }

  describe("When you have a BigDecimal with lots of digits after the .") {
    it("should convert it and round it up when necessary") {
      assert(acmFunctions.bigDecimalTo2Decimals(BigDecimal("12.909")) == 12.91)
    }
    it("should convert it and not round it up") {
      assert(acmFunctions.bigDecimalTo2Decimals(BigDecimal("12.901")) == 12.90)
    }
  }

  describe("When you have a 2 decimal BigDecimal") {
    it("should convert it without applying round up") {
      assert(acmFunctions.bigDecimalTo2Decimals(BigDecimal("12.96")) == 12.96)
    }
  }

  describe("When you have a zero exp BigDecimal") {
    it("should convert it to 0.0") {
      assert(acmFunctions.bigDecimalTo2Decimals(BigDecimal("0E-18")) == 0.0)
    }
  }
}
