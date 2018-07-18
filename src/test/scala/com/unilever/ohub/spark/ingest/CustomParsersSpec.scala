package com.unilever.ohub.spark.ingest

import java.sql.Timestamp
import java.time.format.DateTimeParseException

import com.unilever.ohub.spark.generic.StringFunctions._
import org.scalatest.{ FunSpec, Matchers }
import CustomParsers._
import org.apache.log4j.{ LogManager, Logger }

class CustomParsersSpec extends FunSpec with Matchers {
  implicit val testLogger: Logger = LogManager.getLogger(this.getClass)

  describe("onlyFillLastNameWhenFirstEqualsLastName") {
    it("should return the first name if last name is empty and first not") {
      assert(fillLastNameOnlyWhenFirstEqualsLastName("hans", "", isFirstName = true).equals("hans"))
    }
    it("should return nothing if both names are filled, not empty and first name is selected") {
      assert(fillLastNameOnlyWhenFirstEqualsLastName("hans", "hans", isFirstName = true).equals(""))
    }
    it("should return duplicate name if both names are filled, not empty and last name is selected") {
      assert(fillLastNameOnlyWhenFirstEqualsLastName("hans", "hans", isFirstName = false).equals("hans"))
    }
    it("""should return "" if both names are null""") {
      assert(fillLastNameOnlyWhenFirstEqualsLastName(null, null, isFirstName = false).equals(""))
    }
    it("should return the last name if first name is empty and last not") {
      assert(fillLastNameOnlyWhenFirstEqualsLastName("", "hans", isFirstName = false).equals("hans"))
    }
  }

  describe("parseDateTimeStampOption") {
    it("should parse empty string") {
      assertThrows[IllegalArgumentException](parseDateTimeStampUnsafe(""))
    }

    it("should parse 0 as None") {
      assertThrows[IllegalArgumentException](parseDateTimeStampUnsafe("0"))
    }

    it("should parse 20171215 as 2017-12-15 00:00:00") {
      assert(parseDateTimeStampUnsafe("20171215").toString == "2017-12-15 00:00:00.0")
    }

    it("should parse 20171215 12:13:14 as 2017-12-15 12:13:14") {
      assert(parseDateTimeStampUnsafe("20171215 12:13:14").toString == "2017-12-15 12:13:14.0")
    }

    it("should parse 2017-12-15 12:13:14 as 2017-12-15 12:13:14") {
      assert(parseDateTimeStampUnsafe("2017-12-15 12:13:14").toString == "2017-12-15 12:13:14.0")
    }

    it("should parse 2017/12/15 12:13:14 as 2017-12-15 12:13:14") {
      assert(parseDateTimeStampUnsafe("2017/12/15 12:13:14").toString == "2017-12-15 12:13:14.0")
    }

    it("should parse 2017.12.15 12:13:14 as 2017-12-15 12:13:14") {
      assert(parseDateTimeStampUnsafe("2017.12.15 12:13:14").toString == "2017-12-15 12:13:14.0")
    }

    it("should parse 1531819820081 as 2018-07-17 09:30:20.0 with UTC timezone") {
      assert(parseDateTimeStampUnsafe("1531819820081").toString == "2018-07-17 09:30:20.0")
    }

    it("should return None on other input") {
      assertThrows[IllegalArgumentException](parseDateTimeStampUnsafe("Foo"))
    }
  }

  describe("parseBigDecimalOption") {
    it("should parse an empty string") {
      assertThrows[MatchError](parseBigDecimalUnsafe(""))
    }
    it("should parse -1000,96 as -1000.96") {
      assert(parseBigDecimalUnsafe("-1000,96").toString == "-1000.96")
    }
    it("should parse -1000.96 as -1000.96") {
      assert(parseBigDecimalUnsafe("-1000.96").toString == "-1000.96")
    }
    it("should parse abc as 0") {
      assertThrows[MatchError](parseBigDecimalUnsafe("abc"))
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

  describe("parseBoolUnsafe") {
    it("should parse Y to true") {
      assert(parseBoolUnsafe("Y") == true)
    }

    it("should parse N to false") {
      assert(parseBoolUnsafe("N") == false)
    }

    it("should parse A to true") {
      assert(parseBoolUnsafe("A") == true)
    }

    it("should parse D to false") {
      assert(parseBoolUnsafe("D") == false)
    }

    it("should parse empty string to None") {
      assertThrows[Exception](parseBoolUnsafe(""))
    }

    it("should throw exception on other input") {
      assertThrows[Exception](parseBoolUnsafe("Foo"))
    }
  }

  describe("checkEmailValidity") {
    it("should throw a NPE when provided with null") {
      intercept[NullPointerException] {
        checkEmailValidity(null)
      }
    }
    it("should throw an illegal argument exception when email string is empty") {
      intercept[IllegalArgumentException] {
        checkEmailValidity("")
      }
    }
    it("should throw an illegal argument exception when email contains more then one @") {
      intercept[IllegalArgumentException] {
        checkEmailValidity("hans.kazan@@hotmail.nl")
      }
    }
    it("should throw an illegal argument exception when email has no dot after the @") {
      intercept[IllegalArgumentException] {
        checkEmailValidity("hans.kazan@hotmailnl")
      }
    }
    it("should throw an illegal argument exception when email has non western letters in it like من") {
      intercept[IllegalArgumentException] {
        checkEmailValidity("hans.kazanمن@hotmail.nl")
      }
    }
    it("should return hans.kazan@hotmail.nl") {
      assert(checkEmailValidity("hans.kazan@hotmail.nl") == "hans.kazan@hotmail.nl")
    }
  }

  describe("withinRange") {
    it("should return the provided int when the input is contained in the range") {
      assert(withinRange(Range.inclusive(0, 5))("4") == 4)
    }
    intercept[IllegalArgumentException] {
      withinRange(Range.inclusive(0, 5))("8")
    }.getMessage shouldBe s"Input value '8' not within provided range 'Range(0, 1, 2, 3, 4, 5)'"
    intercept[IllegalArgumentException] {
      withinRange(Range.inclusive(0, 5))("abc")
    }
  }

  describe("parseDateTimeForPattern") {
    it("should parse the date time correctly in") {
      assert(parseDateTimeForPattern()("2018-01-12 13:20:58.70") == Timestamp.valueOf("2018-01-12 13:20:58.7"))
    }
    intercept[DateTimeParseException] {
      parseDateTimeForPattern()("some-illegal-value")
    }.getMessage shouldBe "Text 'some-illegal-value' could not be parsed at index 0"
  }

  describe("concatNames") {
    it("should return null when first and last name are null") {
      assert(concatNames(null, null) == null)
    }
    it("should return null when first and last name and email address are null") {
      assert(concatNames(null, null, null) == null)
    }
    it("should return null when first and last name are empty") {
      assert(concatNames("", "") == null)
    }
    it("should return null when first and last name are empty and email address is null") {
      assert(concatNames("", "", null) == null)
    }
    it("should return kazan when first name is null and last name is kazan and email address is null") {
      assert(concatNames(null, "kazan", null) == "kazan")
    }
    it("should return null when first is hans, last name is null and email address is null") {
      assert(concatNames("hans", null, null) == "hans")
    }
    it("should return kazan when first name is null and last name is kazan and email address is empty") {
      assert(concatNames(null, "kazan") == "kazan")
    }
    it("should return null when first is hans, last name is null and email address is empty") {
      assert(concatNames("hans", null) == "hans")
    }
    it("should return อติพรสว่างเมฆ when first is อติพร, last name is สว่างเมฆ and email address is empty") {
      assert(concatNames("อติพร", "สว่างเมฆ", null) == "สว่างเมฆอติพร")
    }
    it("should return hans kazan if both string contain Hans Kazan in upper or lower case") {
      assert(concatNames("hans kazan", "hans kazan") == "hans kazan")
    }
    it("should return hanskazan if first name is kazan an last name is hans") {
      assert(concatNames("kazan", "hans") == "hanskazan")
    }
    it("should return hanskazan if first name is hans an last name is kazan") {
      assert(concatNames("hans", "kazan") == "hanskazan")
    }
    it("should return hanskazan if first and last name are empty and email is hanskazan@hotmail.com") {
      assert(concatNames("", "", "hanskazan@hotmail.com") == "hanskazan")
    }
    it("should return hanskazan if first and last name are empty and email is hans.kazan@hotmail.com") {
      assert(concatNames("", "", "hans.kazan@hotmail.com") == "hanskazan")
    }
    it("should return hanskazan if first and last name are empty and email is kazan.hans@hotmail.com") {
      assert(concatNames("", "", "kazan.hans@hotmail.com") == "hanskazan")
    }
    it("should return hanskazan if first and last name are empty and email is hans-kazan@hotmail.com") {
      assert(concatNames("", "", "hans-kazan@hotmail.com") == "hanskazan")
    }
    it("should return hanskazan if first and last name are empty and email is kazan-hans@hotmail.com") {
      assert(concatNames("", "", "kazan-hans@hotmail.com") == "hanskazan")
    }
    it("should return hanskazan if first and last name are empty and email is hans_kazan@hotmail.com") {
      assert(concatNames("", "", "hans_kazan@hotmail.com") == "hanskazan")
    }
    it("should return hanskazan if first and last name are empty and email is kazan_hans@hotmail.com") {
      assert(concatNames("", "", "kazan_hans@hotmail.com") == "hanskazan")
    }
    it("should return hanskazan if first and last name are null and email is hanskazan@hotmail.com") {
      assert(concatNames(null, null, "hanskazan@hotmail.com") == "hanskazan")
    }
    it("should return hanskazan if first and last name are null and email is hans.kazan@hotmail.com") {
      assert(concatNames(null, null, "hans.kazan@hotmail.com") == "hanskazan")
    }
    it("should return hanskazan if first and last name are null and email is kazan.hans@hotmail.com") {
      assert(concatNames(null, null, "kazan.hans@hotmail.com") == "hanskazan")
    }
    it("should return hanskazan if first and last name are null and email is hans-kazan@hotmail.com") {
      assert(concatNames(null, null, "hans-kazan@hotmail.com") == "hanskazan")
    }
    it("should return hanskazan if first and last name are null and email is kazan-hans@hotmail.com") {
      assert(concatNames(null, null, "kazan-hans@hotmail.com") == "hanskazan")
    }
    it("should return hanskazan if first and last name are null and email is hans_kazan@hotmail.com") {
      assert(concatNames(null, null, "hans_kazan@hotmail.com") == "hanskazan")
    }
    it("should return hanskazan if first and last name are null and email is kazan_hans@hotmail.com") {
      assert(concatNames(null, null, "kazan_hans@hotmail.com") == "hanskazan")
    }
  }
}
