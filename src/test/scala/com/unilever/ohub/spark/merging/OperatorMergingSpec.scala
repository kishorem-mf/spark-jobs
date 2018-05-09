package com.unilever.ohub.spark.merging

import com.unilever.ohub.spark.SparkJobSpec
import com.unilever.ohub.spark.domain.entity.{ Operator, TestOperators }
import org.apache.spark.sql.Dataset
import com.unilever.ohub.spark.SharedSparkSession.spark

class OperatorMergingSpec extends SparkJobSpec with TestOperators {
  import spark.implicits._

  private val MATCHES: Dataset[MatchingResult] = Seq(
    matchingResultWithSourceName("a", "b", "NL"),
    matchingResultWithSourceName("a", "c", "NL"),
    matchingResultWithSourceName("x", "y", "US")
  ).toDataset
  private val OPERATORS = Seq(
    defaultOperatorWithSourceNameAndCountryCode("a", "NL"),
    defaultOperatorWithSourceNameAndCountryCode("b", "NL"),
    defaultOperatorWithSourceNameAndCountryCode("c", "NL"),
    defaultOperatorWithSourceNameAndCountryCode("d", "NL"), // not in MATCHES
    defaultOperatorWithSourceNameAndCountryCode("x", "US"),
    defaultOperatorWithSourceNameAndCountryCode("y", "US")
  ).toDataset

  private def matchingResultWithSourceName(source: String, target: String, countryCode: String) = {
    def concat(sourceName: String, cc: String) = s"$cc~$sourceName~${defaultOperator.sourceEntityId}"

    MatchingResult(concat(source, countryCode), concat(target, countryCode), countryCode)
  }

  describe("groupMatchedOperators") {
    it("should group all operators based on the target from the matching algorithm") {

      val res = OperatorMerging.groupMatchedOperators(spark, OPERATORS, MATCHES)
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
      val matchedOperators: Dataset[Seq[Operator]] = Seq(
        Seq(
          defaultOperatorWithSourceNameAndCountryCode("a", "NL"),
          defaultOperatorWithSourceNameAndCountryCode("b", "NL"),
          defaultOperatorWithSourceNameAndCountryCode("c", "NL")),
        Seq(
          defaultOperatorWithSourceNameAndCountryCode("x", "US"),
          defaultOperatorWithSourceNameAndCountryCode("y", "US"))
      ).toDataset

      val res = OperatorMerging.findUnmatchedOperators(spark, OPERATORS, matchedOperators)
        .collect

      res.length shouldBe 1
      res.head.length shouldBe 1
      res.head.head.concatId shouldBe s"NL~d~${defaultOperator.sourceEntityId}"
    }
  }

  describe("transform") {
    it("should create ohub ids for all matched and unmatched operators") {
      val sourcePreferences = Map("a" -> 1, "b" -> 2, "c" -> 3, "d" -> 4, "x" -> 4, "y" -> 5)
      val res = OperatorMerging.transform(spark, OPERATORS, MATCHES, sourcePreferences)
        .collect
        .sortBy(_.sourceName)

      res.length shouldBe 6
      res.map(r ⇒ (r.sourceName, r.isGoldenRecord)) should contain inOrderOnly (
        ("a", true), ("b", false), ("c", false), ("d", true), ("x", true), ("y", false)
      )

      val firstGroup = res.take(3)
      firstGroup.map(_.ohubId.get) should contain only firstGroup.head.ohubId.get

      val secondGroup = res.drop(4)
      secondGroup.map(_.ohubId.get) should contain only secondGroup.head.ohubId.get
    }
  }
}
