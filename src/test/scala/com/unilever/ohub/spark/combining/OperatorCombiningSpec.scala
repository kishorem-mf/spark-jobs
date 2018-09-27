package com.unilever.ohub.spark.combining

import com.unilever.ohub.spark.SparkJobSpec
import com.unilever.ohub.spark.SharedSparkSession.spark
import com.unilever.ohub.spark.domain.entity.{ Operator, TestOperators }

class OperatorCombiningSpec extends SparkJobSpec with TestOperators {
  import spark.implicits._

  describe("combining") {
    it("should combine emtpy datasets") {
      val d1 = spark.emptyDataset[Operator]
      val d2 = spark.emptyDataset[Operator]

      val res = OperatorCombining.transform(spark, d1, d2).collect()

      assert(res.length === 0)
    }

    it("should combine two operator datasets") {
      val d1 = Seq(
        defaultOperatorWithSourceEntityId("source-entity-id-a"),
        defaultOperatorWithSourceEntityId("source-entity-id-b")
      ).toDataset
      val d2 = defaultOperatorWithSourceEntityId("source-entity-id-c").toDataset

      val res = OperatorCombining.transform(spark, d1, d2).collect()

      assert(res.length === 3)
      res.map(_.sourceEntityId) should contain theSameElementsAs Seq("source-entity-id-a", "source-entity-id-b", "source-entity-id-c")
    }

    it("should combine two operator datasets and take the latest concatId") {
      val d1 = Seq(
        defaultOperatorWithSourceEntityId("source-entity-id-a"),
        defaultOperatorWithSourceEntityId("source-entity-id-b").copy(name = "foo")
      ).toDataset
      val d2 = Seq(
        defaultOperatorWithSourceEntityId("source-entity-id-c"),
        defaultOperatorWithSourceEntityId("source-entity-id-b").copy(name = "bar")
      ).toDataset

      val res = OperatorCombining.transform(spark, d1, d2).collect()

      assert(res.length === 3)
      res.map(_.sourceEntityId) should contain theSameElementsAs Seq("source-entity-id-a", "source-entity-id-b", "source-entity-id-c")
      res.find(_.sourceEntityId == "source-entity-id-b").get.name shouldBe "bar"
    }
  }
}
