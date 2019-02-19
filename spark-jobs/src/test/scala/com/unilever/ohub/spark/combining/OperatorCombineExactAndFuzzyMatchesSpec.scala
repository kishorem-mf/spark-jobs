package com.unilever.ohub.spark.combining

import com.unilever.ohub.spark.SparkJobSpec
import com.unilever.ohub.spark.SharedSparkSession.spark
import com.unilever.ohub.spark.domain.entity.{ Operator, TestOperators }

class OperatorCombineExactAndFuzzyMatchesSpec extends SparkJobSpec with TestOperators {

  import spark.implicits._

  describe("combining") {
    it("should combine emtpy datasets") {
      val d1 = spark.emptyDataset[Operator]
      val d2 = spark.emptyDataset[Operator]
      val d3 = spark.emptyDataset[Operator]

      val res = OperatorCombineExactAndFuzzyMatches.transform(spark, d1, d2, d3).collect()

      assert(res.length === 0)
    }

    it("should combine two operator datasets") {
      val d1 = Seq(
        defaultOperatorWithSourceEntityId("source-entity-id-a"),
        defaultOperatorWithSourceEntityId("source-entity-id-b")
      ).toDataset
      val d2 = defaultOperatorWithSourceEntityId("source-entity-id-c")
        .toDataset
      val d3 = defaultOperatorWithSourceEntityId("source-entity-id-d")
        .toDataset

      val res = OperatorCombineExactAndFuzzyMatches.transform(spark, d1, d2, d3).collect()

      assert(res.length === 4)
      res.map(_.sourceEntityId) should contain theSameElementsAs Seq(
        "source-entity-id-a",
        "source-entity-id-b",
        "source-entity-id-c",
        "source-entity-id-d"
      )
    }

    it("should combine two operator datasets and take the latest concatId") {
      val d1 = Seq(
        defaultOperatorWithSourceEntityId("source-entity-id-a"),
        defaultOperatorWithSourceEntityId("source-entity-id-b").copy(
          name = Some("foo"),
          ohubCreated = java.sql.Timestamp.valueOf("2019-05-24 12:00:00.0")
        )
      ).toDataset
      val d2 = Seq(
        defaultOperatorWithSourceEntityId("source-entity-id-c"),
        defaultOperatorWithSourceEntityId("source-entity-id-b").copy(
          name = Some("bar"),
          ohubCreated = java.sql.Timestamp.valueOf("2019-05-25 12:00:00.0"))
      ).toDataset
      val d3 = Seq(
        defaultOperatorWithSourceEntityId("source-entity-id-d")
      ).toDataset

      val res = OperatorCombineExactAndFuzzyMatches.transform(spark, d1, d2, d3).collect()

      assert(res.length === 4)
      res.map(_.sourceEntityId) should contain theSameElementsAs Seq(
        "source-entity-id-a",
        "source-entity-id-b",
        "source-entity-id-c",
        "source-entity-id-d")
      res.find(_.sourceEntityId == "source-entity-id-b").get.name shouldBe Some("bar")
    }
  }
}
