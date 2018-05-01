package com.unilever.ohub.spark.merging

import com.unilever.ohub.spark.SparkJobSpec
import com.unilever.ohub.spark.domain.entity.TestOperators
import org.apache.spark.sql.Dataset
import com.unilever.ohub.spark.SharedSparkSession.spark

class OperatorMergingSpec extends SparkJobSpec with TestOperators {

  import spark.implicits._

  def matchingResultWithSourceName(source: String, target: String, countryCode: String) = {
    def concat(sourceName: String, cc: String) = s"$cc~$sourceName~${defaultOperator.sourceEntityId}"

    MatchingResult(concat(source, countryCode), concat(target, countryCode), countryCode)
  }

  describe("groupMatchedOperators") {
    it("should group all operators based on the target from the matching algorithm") {
      val matches: Dataset[MatchingResult] = Seq(
        matchingResultWithSourceName("a", "b", "NL"),
        matchingResultWithSourceName("a", "c", "NL"),
        matchingResultWithSourceName("x", "y", "US")
      ).toDataset
      val operators = Seq(
        defaultOperator,
        defaultOperatorWithSourceNameAndCountryCode("a", "NL"),
        defaultOperatorWithSourceNameAndCountryCode("b", "NL"),
        defaultOperatorWithSourceNameAndCountryCode("c", "NL"),
        defaultOperatorWithSourceNameAndCountryCode("d", "NL"),
        defaultOperatorWithSourceNameAndCountryCode("x", "US"),
        defaultOperatorWithSourceNameAndCountryCode("y", "US")
      ).toDataset

      val res = OperatorMerging.groupMatchedOperators(spark, operators, matches)
        .collect
        .sortBy(_.length)

      res.length shouldBe 2
      res.head.length shouldBe 2
      res.head.map(_.concatId) should contain allOf (
        s"US~x~${defaultOperator.sourceEntityId}",
        s"US~y~${defaultOperator.sourceEntityId}"
      )
      res.last.length shouldBe 3
      res.last.map(_.concatId) should contain allOf (
        s"NL~a~${defaultOperator.sourceEntityId}",
        s"NL~b~${defaultOperator.sourceEntityId}",
        s"NL~c~${defaultOperator.sourceEntityId}"
      )
    }
  }
  describe("findUnmatchedOperators") {
    it("should return all unmatched operators") {

    }
  }

  describe("transform") {
    it("should create ohub ids for all matched and unmatched operators") {

    }
  }
}
