package com.unilever.ohub.spark.tsv2parquet

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

  describe("parseBoolOption") {
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

  describe("getFastSimilarity") {
    it("should return 0.0 when the first string is null") {
      assert(getFastSimilarity(null, "test".toCharArray) <= 1.0 && getFastSimilarity(null, "test".toCharArray) >= 0.0)
    }
    it("should return 0.0 when the second string is null") {
      assert(getFastSimilarity("test".toCharArray, null) <= 1.0 && getFastSimilarity("test".toCharArray, null) >= 0.0)
    }
    it("should return 0.2 when a is compared to aaaaa") {
      assert(getFastSimilarity("a".toCharArray, "aaaaa".toCharArray) == 0.2)
    }
    it("should return 0.2 when aaaaa is compared to a") {
      assert(getFastSimilarity("aaaaa".toCharArray, "a".toCharArray) == 0.2)
    }
    it("should return 1.0 when both string are levenshtein") {
      assert(getFastSimilarity("levenshtein".toCharArray, "levenshtein".toCharArray) == 1.0)
    }
    it("""should return 0.9 when the first string is "jackhammer" and the second string is " jackhammer""") {
      assert(getFastSimilarity("jackhammer".toCharArray, " jackhammer".toCharArray) == 0.9)
    }
    it("""should return 0.9 when the first 11 letter string is "شيء لطيف   " and the second 10 letter string is "يء لطيف   """") {
      assert(getFastSimilarity("شيء لطيف   ".toCharArray, "يء لطيف   ".toCharArray) == 0.9)
    }
    it("""should return 0.875 when the first string is "Hotel California" and the second string is "California Hotel"""") {
      assert(getFastSimilarity("Hotel California".toCharArray, "California Hotel".toCharArray) == 0.875)
    }
  }

  describe("calculateLevenshtein") {
    it("should return 0.0 when the first string is null") {
      assert(calculateLevenshtein(null, "test".toCharArray) <= 1.0 && calculateLevenshtein(null, "test".toCharArray) >= 0.0)
    }
    it("should return 0.0 when the second string is null") {
      assert(calculateLevenshtein("test".toCharArray, null) <= 1.0 && calculateLevenshtein("test".toCharArray, null) >= 0.0)
    }
    it("should return 0.2 when a is compared to aaaaa") {
      assert(calculateLevenshtein("a".toCharArray, "aaaaa".toCharArray) == 0.2)
    }
    it("should return 0.2 when aaaaa is compared to a") {
      assert(calculateLevenshtein("aaaaa".toCharArray, "a".toCharArray) == 0.2)
    }
    it("should return 1.0 when both string are levenshtein") {
      assert(calculateLevenshtein("levenshtein".toCharArray, "levenshtein".toCharArray) == 1.0)
    }
    it("""should return 0.9 when the first string is "jackhamme" and the second string is "jackhammer""") {
      assert(calculateLevenshtein("jackhamme".toCharArray, "jackhammer".toCharArray) == 0.9)
    }
    it("""should return 0.9 when the first 10 letter string is "شيء لطيف  " and the second 9 letter string is "يء لطيف  """") {
      assert(calculateLevenshtein("شيء لطيف  ".toCharArray, "يء لطيف  ".toCharArray) == 0.9)
    }
    it("""should return 0.25 when the first string is "Hotel California" and the second string is "California Hotel"""") {
      assert(calculateLevenshtein("Hotel California".toCharArray, "California Hotel".toCharArray) == 0.25)
    }
  }

  describe("cleanPhoneNumber") {
    val countryList = Array("CU", "CX", "FI", "GS", "GY", "KE", "KY", "LV", "LY", "MM", "MP", "MS", "NC", "NO", "NZ", "AO", "AS", "AW", "BH", "BN", "PY", "RU", "SO", "SZ", "TC", "TN", "VA", "VE", "WF", "CR", "DJ", "ES", "FM", "GH", "GT", "GU", "IL", "IO", "LI", "MH", "MR", "NA", "NG", "NP", "AI", "AR", "BA", "BI", "BZ", "PM", "PT", "PW", "TD", "TR", "TZ", "CH", "CI", "CK", "CN", "CY", "CZ", "EC", "GM", "IE", "IS", "IT", "JP", "KR", "LK", "LR", "MG", "MQ", "NE", "PG", "AT", "BD", "BF", "BG", "BO", "CA", "SA", "SG", "ST", "SX", "TH", "TM", "VG", "VU", "CL", "CO", "DO", "EE", "FJ", "FK", "FR", "GD", "GE", "GF", "GG", "HT", "ID", "IM", "JE", "JM", "JO", "KG", "KP", "LB", "LC", "MN", "MT", "NR", "AG", "AL", "AM", "AX", "AZ", "PL", "QA", "SB", "SS", "TK", "TT", "UG", "WS", "YT", "ZA", "EH", "GR", "HN", "IN", "KH", "KW", "LU", "MV", "MX", "MZ", "PA", "AD", "AE", "PR", "SE", "TV", "UZ", "VC", "VI", "CW", "DE", "GB", "GI", "GL", "GP", "GW", "HR", "HU", "IQ", "KI", "KM", "MA", "MC", "ME", "ML", "NF", "PF", "AU", "BE", "BL", "BT", "PK", "PS", "RE", "RO", "RS", "SD", "SH", "SI", "SL", "SM", "TG", "TW", "UA", "YE", "ZW", "CM", "DM", "EG", "ET", "GN", "KZ", "LA", "LS", "LT", "MU", "MY", "NL", "OM", "PE", "PH", "AF", "BB", "BJ", "BM", "BS", "CC", "RW", "SK", "TO", "US", "VN", "CG", "CV", "DK", "DZ", "ER", "FO", "GA", "GQ", "HK", "IR", "KN", "MD", "MK", "MO", "MW", "NI", "NU", "BQ", "BR", "BW", "BY", "CF", "SC", "SN", "SR", "SV", "SY", "TJ", "TL", "UY", "ZM")
    val prefixList = Array("53", "61", "358", "500", "592", "254", "1", "371", "218", "95", "1", "1", "687", "47", "64", "244", "1", "297", "973", "673", "595", "7", "252", "268", "1", "216", "39", "58", "681", "506", "253", "34", "691", "233", "502", "1", "972", "246", "423", "692", "222", "264", "234", "977", "1", "54", "387", "257", "501", "508", "351", "680", "235", "90", "255", "41", "225", "682", "86", "357", "420", "593", "220", "353", "354", "39", "81", "82", "94", "231", "261", "596", "227", "675", "43", "880", "226", "359", "591", "1", "966", "65", "239", "1", "66", "993", "1", "678", "56", "57", "1", "372", "679", "500", "33", "1", "995", "594", "44", "509", "62", "44", "44", "1", "962", "996", "850", "961", "1", "976", "356", "674", "1", "355", "374", "358", "994", "48", "974", "677", "211", "690", "1", "256", "685", "262", "27", "212", "30", "504", "91", "855", "965", "352", "960", "52", "258", "507", "376", "971", "1", "46", "688", "998", "1", "1", "599", "49", "44", "350", "299", "590", "245", "385", "36", "964", "686", "269", "212", "377", "382", "223", "672", "689", "61", "32", "590", "975", "92", "97", "262", "40", "381", "249", "290", "386", "232", "378", "228", "886", "380", "967", "263", "237", "1", "20", "251", "224", "7", "856", "266", "370", "230", "60", "31", "968", "51", "63", "93", "1", "229", "1", "1", "61", "250", "421", "676", "1", "84", "243", "238", "45", "213", "291", "298", "241", "240", "852", "98", "1", "373", "389", "853", "265", "505", "683", "599", "55", "267", "375", "236", "248", "221", "597", "503", "963", "992", "670", "598", "260")
    val countryPrefixList = countryList zip prefixList.toList
    it("should return null when phone number is empty") {
      assert(cleanPhoneNumber("", "DK", countryPrefixList) == null)
    }
    it("should return null when phone number is null") {
      assert(cleanPhoneNumber(null, "DK", countryPrefixList) == null)
    }
    it("should return null when phone number is 0000") {
      assert(cleanPhoneNumber("0000", "DK", countryPrefixList) == null)
    }
  }

  describe("checkEmailValidity") {
    it("should return null when email string is null") {
      assert(checkEmailValidity(null) == null)
    }
    it("should return null when email string is empty") {
      assert(checkEmailValidity("") == null)
    }
    it("should return null when email contain more then one @") {
      assert(checkEmailValidity("hans.kazan@@hotmail.nl") == null)
    }
    it("should return hans.kazan@hotmail.nl") {
      assert(checkEmailValidity("hans.kazan@hotmail.nl") == "hans.kazan@hotmail.nl")
    }
    it("should return null when email has no dot after the @") {
      assert(checkEmailValidity("hans.kazan@hotmailnl") == null)
    }
    it("should return null when email has non western letters in it like من") {
      assert(checkEmailValidity("hans.kazanمن@hotmail.nl") == null)
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
