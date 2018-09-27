package com.unilever.ohub.spark.merging

import com.unilever.ohub.spark.SparkJobSpec
import com.unilever.ohub.spark.SharedSparkSession.spark
import com.unilever.ohub.spark.domain.entity.TestOperators

class OperatorUpdateGoldenRecordSpec extends SparkJobSpec with TestOperators {
  import spark.implicits._

  describe("marking golden record") {
    it("should pick the operator with highest sourcePreference") {
      val sourcePreferences = Map(
        "sourceA" -> 2,
        "sourceB" -> 1
      )
      val operators = Seq(
        defaultOperatorWithSourceName("sourceA"),
        defaultOperatorWithSourceName("sourceB")
      )
      val golden = OperatorUpdateGoldenRecord.markGoldenRecord(sourcePreferences)(operators)

      assert(golden.size === 2)
      assert(!golden.find(_.sourceName == "sourceA").get.isGoldenRecord)
      assert(golden.find(_.sourceName == "sourceB").get.isGoldenRecord)
    }

    it("should pick the operator created last if sourcePreferences are equal") {
      val sourcePreferences = Map(
        "sourceA" -> 1,
        "sourceB" -> 1
      )
      val operators = Seq(
        defaultOperatorWithSourceName("sourceA"),
        defaultOperatorWithSourceName("sourceB")
      )
      val golden = OperatorUpdateGoldenRecord.markGoldenRecord(sourcePreferences)(operators)

      assert(golden.size === 2)
      assert(!golden.find(_.sourceName == "sourceA").get.isGoldenRecord)
      assert(golden.find(_.sourceName == "sourceB").get.isGoldenRecord)
    }
  }

  describe("updating golden record") {
    it("should not change anything if not needed") {
      val sourcePreferences = Map(
        "sourceA" -> 2,
        "sourceB" -> 1
      )

      val a = defaultOperatorWithSourceName("sourceA").copy(ohubId = Some("a"))
      val b = defaultOperatorWithSourceName("sourceB").copy(isGoldenRecord = true, ohubId = Some("a"))

      val operators = Seq(a, b).toDataset
      val updated = OperatorUpdateGoldenRecord.transform(spark, operators, sourcePreferences).collect

      assert(updated.find(_.sourceName == "sourceA").get === a)
      assert(updated.find(_.sourceName == "sourceB").get === b)
    }

    it("should not change anything if needed") {
      val sourcePreferences = Map(
        "sourceA" -> 1,
        "sourceB" -> 2
      )

      val a = defaultOperatorWithSourceName("sourceA").copy(ohubId = Some("a"))
      val b = defaultOperatorWithSourceName("sourceB").copy(isGoldenRecord = true, ohubId = Some("a"))

      val operators = Seq(a, b).toDataset

      val updated = OperatorUpdateGoldenRecord.transform(spark, operators, sourcePreferences).collect

      assert(updated.find(_.sourceName == "sourceA").get.isGoldenRecord)
      assert(!updated.find(_.sourceName == "sourceB").get.isGoldenRecord)
    }
  }
}
