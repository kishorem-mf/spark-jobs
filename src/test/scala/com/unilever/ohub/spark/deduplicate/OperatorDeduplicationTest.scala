package com.unilever.ohub.spark.deduplicate

import java.sql.Timestamp
import java.util.UUID

import com.unilever.ohub.spark.SparkJobSpec
import com.unilever.ohub.spark.domain.entity.Operator
import com.unilever.ohub.spark.SharedSparkSession.spark

class OperatorDeduplicationTest extends SparkJobSpec {

  import spark.implicits._


  describe("deduplication") {

    it("should return integrated if daily is empty") {
      val integrated = defaultOperatorRecord.toDataset

      val daily = spark.emptyDataset[Operator]

      val (updatedDs, dailyNewDs) = OperatorDeduplication.transform(spark, integrated, daily)

      val updated = updatedDs.collect
      val dailyNew = dailyNewDs.collect
      assert(updated.length === 1)
      assert(dailyNew.length === 0)
    }

    it("should find no duplicates if none are there") {
      val integrated = Seq(
        defaultOperatorRecord.copy(concatId = "a"),
        defaultOperatorRecord.copy(concatId = "b")
      ).toDataset

      val daily = defaultOperatorRecord.copy(concatId = "c").toDataset

      val (updatedDs, dailyNewDs) = OperatorDeduplication.transform(spark, integrated, daily)

      val updated = updatedDs.collect
      val dailyNew = dailyNewDs.collect
      assert(updated.length === 2)
      assert(dailyNew.length === 1)

      updated.map(_.concatId) should contain theSameElementsAs Seq("a", "b")
      assert(dailyNew.head.concatId === "c")
    }

    it("should return a deduplicated dataset if there are duplicates") {
      val integrated = Seq(
        defaultOperatorRecord.copy(concatId = "a"),
        defaultOperatorRecord.copy(concatId = "b")
      ).toDataset
      val daily = defaultOperatorRecord.copy(concatId = "a").toDataset

      val (updatedDs, dailyNewDs) = OperatorDeduplication.transform(spark, integrated, daily)

      val updated = updatedDs.collect
      val dailyNew = dailyNewDs.collect
      assert(updated.length === 2)
      assert(dailyNew.length === 0)

      updated.map(_.concatId) should contain theSameElementsAs Seq("a", "b")
    }

    it("return a deduplicated dataset with the newest record if there are duplicates") {
      val integrated = Seq(
        defaultOperatorRecord.copy(concatId = "a", dateUpdated = Some(Timestamp.valueOf("2017-05-25 12:00:00"))),
        defaultOperatorRecord.copy(concatId = "b")
      ).toDataset

      val newTimestamp = Some(Timestamp.valueOf("2017-06-25 12:00:00"))
      val daily = defaultOperatorRecord.copy(concatId = "a", dateUpdated = newTimestamp).toDataset

      val (updatedDs, dailyNewDs) = OperatorDeduplication.transform(spark, integrated, daily)

      val updated = updatedDs.collect
      val dailyNew = dailyNewDs.collect
      assert(updated.length === 2)
      assert(dailyNew.length === 0)

      updated.map(_.concatId) should contain theSameElementsAs Seq("a", "b")
      assert(updated.find(_.concatId == "a").get.dateUpdated === newTimestamp)
    }
  }
}
