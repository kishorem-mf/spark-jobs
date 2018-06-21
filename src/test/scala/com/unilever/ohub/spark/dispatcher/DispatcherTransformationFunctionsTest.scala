package com.unilever.ohub.spark.dispatcher

import java.sql.Timestamp

import com.unilever.ohub.spark.SimpleSpec

class DispatcherTransformationFunctionsTest extends SimpleSpec with DispatcherTransformationFunctions {

  final val CUSTOM_PATTERN = "yyyy-MM-dd"
  final val TIMESTAMP = new Timestamp(117, 10, 16, 18, 9, 49, 0)
  final val FORMATTED_TIMESTAMP = "2017-11-16 18:09:49"
  final val FORMATTED_DATE = "2017-11-16"

  describe("formatWithPattern") {
    it(s"format with default pattern - '$DATE_FORMAT'") {
      formatWithPattern()(TIMESTAMP) shouldEqual FORMATTED_TIMESTAMP
    }

    it(s"format with custom format '$CUSTOM_PATTERN'") {
      formatWithPattern(CUSTOM_PATTERN)(TIMESTAMP) shouldEqual FORMATTED_DATE
    }
  }

  describe("boolAsString") {
    it("should format a 'true'") {
      BOOL_AS_STRING(true) shouldEqual YES
    }

    it("should format a 'false'") {
      BOOL_AS_STRING(false) shouldEqual NO
    }
  }

  describe("OptBooleanOps operations") {
    it("should extend with mapToUNYOpt") {
      Option(true).mapToYNOpt shouldEqual Some(YES)
      Option(false).mapToYNOpt shouldEqual Some(NO)
    }

    it("should extend with def mapToUNYOpt") {
      Option.empty[Boolean].mapToUNYOpt shouldEqual Some(UNKNOWN)
      Option(true).mapToUNYOpt shouldEqual Some(YES)
      Option(false).mapToUNYOpt shouldEqual Some(NO)
    }
  }

  describe("BooleanOps operations") {
    it("should extend with mapToYN") {
      true.mapToYN shouldEqual YES
      false.mapToYN shouldEqual NO
    }

    it("should extend with mapToYNOpt") {
      true.mapToYNOpt shouldEqual Some(YES)
      false.mapToYNOpt shouldEqual Some(NO)
    }
  }

  describe("OptTimestampOps operations") {
    it("should extend with mapWithDefaultPatternOpt") {
      Option(TIMESTAMP).mapWithDefaultPatternOpt shouldEqual Some(FORMATTED_TIMESTAMP)
    }
  }

  describe("TimestampOps operations") {
    it("should extend with mapWithDefaultPattern") {
      TIMESTAMP.mapWithDefaultPattern shouldEqual FORMATTED_TIMESTAMP
    }

    it("should extend with mapWithDefaultPatternOpt") {
      TIMESTAMP.mapWithDefaultPatternOpt shouldEqual Some(FORMATTED_TIMESTAMP)
    }
  }
}
