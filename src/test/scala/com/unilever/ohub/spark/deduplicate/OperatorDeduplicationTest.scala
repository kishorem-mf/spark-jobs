package com.unilever.ohub.spark.deduplicate

import java.sql.Timestamp

import com.unilever.ohub.spark.SparkJobSpec
import com.unilever.ohub.spark.domain.entity.{ Operator, TestOperators }
import com.unilever.ohub.spark.SharedSparkSession.spark

class OperatorDeduplicationTest extends SparkJobSpec with TestOperators {

  import spark.implicits._

  describe("deduplication") {

    it("should return integrated if daily is empty") {
      val integrated = defaultOperator.toDataset

      val daily = spark.emptyDataset[Operator]

      val (updatedDs, dailyNewDs) = OperatorDeduplication.transform(spark, integrated, daily)

      val updated = updatedDs.collect
      val dailyNew = dailyNewDs.collect
      assert(updated.length === 1)
      assert(dailyNew.length === 0)
    }

    it("should find no duplicates if none are there") {
      val integrated = Seq(
        defaultOperatorWithSourceEntityId("a"),
        defaultOperatorWithSourceEntityId("b")
      ).toDataset

      val daily = defaultOperatorWithSourceEntityId("c").toDataset

      val (updatedDs, dailyNewDs) = OperatorDeduplication.transform(spark, integrated, daily)

      val updated = updatedDs.collect
      val dailyNew = dailyNewDs.collect
      assert(updated.length === 2)
      assert(dailyNew.length === 1)

      updated.map(_.sourceEntityId) should contain theSameElementsAs Seq("a", "b")
      assert(dailyNew.head.sourceEntityId === "c")
    }

    it("should return a deduplicated dataset if there are duplicates") {
      val integrated = Seq(
        defaultOperatorWithSourceEntityId("a"),
        defaultOperatorWithSourceEntityId("b")
      ).toDataset
      val daily = defaultOperatorWithSourceEntityId("a").toDataset

      val (updatedDs, dailyNewDs) = OperatorDeduplication.transform(spark, integrated, daily)

      val updated = updatedDs.collect
      val dailyNew = dailyNewDs.collect
      assert(updated.length === 2)
      assert(dailyNew.length === 0)

      updated.map(_.sourceEntityId) should contain theSameElementsAs Seq("a", "b")
    }

    it("return a deduplicated dataset with the newest record if there are duplicates") {
      val integrated = Seq(
        defaultOperatorWithSourceEntityId("a").copy(dateUpdated = Some(Timestamp.valueOf("2017-05-25 12:00:00"))),
        defaultOperatorWithSourceEntityId("b")
      ).toDataset

      val newTimestamp = Some(Timestamp.valueOf("2017-06-25 12:00:00"))
      val daily = defaultOperatorWithSourceEntityId("a").copy(dateUpdated = newTimestamp).toDataset

      val (updatedDs, dailyNewDs) = OperatorDeduplication.transform(spark, integrated, daily)

      val updated = updatedDs.collect
      val dailyNew = dailyNewDs.collect
      assert(updated.length === 2)
      assert(dailyNew.length === 0)

      updated.map(_.sourceEntityId) should contain theSameElementsAs Seq("a", "b")
      assert(updated.find(_.sourceEntityId == "a").get.dateUpdated === newTimestamp)
    }
  }
}
