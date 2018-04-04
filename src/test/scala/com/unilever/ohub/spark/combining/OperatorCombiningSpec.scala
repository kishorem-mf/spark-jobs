package com.unilever.ohub.spark.combining

import com.unilever.ohub.spark.SparkJobSpec
import com.unilever.ohub.spark.SharedSparkSession.spark
import com.unilever.ohub.spark.domain.entity.Operator

class OperatorCombiningSpec extends SparkJobSpec {
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
        defaultOperatorRecord.copy(concatId = "a"),
        defaultOperatorRecord.copy(concatId = "b"))
        .toDataset
      val d2 = defaultOperatorRecord.copy(concatId = "c").toDataset

      val res = OperatorCombining.transform(spark, d1, d2).collect()

      assert(res.length === 3)
      res.map(_.concatId) should contain theSameElementsAs Seq("a", "b", "c")
    }
  }

}
