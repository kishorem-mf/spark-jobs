package com.unilever.ohub.spark.merging

import com.unilever.ohub.spark.SparkJobSpec

class OperatorUpdateGoldenRecordSpec extends SparkJobSpec {

  describe("picking golden record") {
    it("should pick the operator with highest sourcePreference") {
      val sourcePreferences = Map(
        "sourceA" -> 1,
        "sourceB" -> 2
      )
      val operators = Seq(
        defaultOperatorRecord.copy(sourceName = "sourceA"),
        defaultOperatorRecord.copy(sourceName = "sourceB")
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
        defaultOperatorRecord.copy(sourceName = "sourceA"),
        defaultOperatorRecord.copy(sourceName = "sourceB")
      )
      val golden = OperatorUpdateGoldenRecord.markGoldenRecord(sourcePreferences)(operators)

      assert(golden.size === 2)
      assert(!golden.find(_.sourceName == "sourceA").get.isGoldenRecord)
      assert(golden.find(_.sourceName == "sourceB").get.isGoldenRecord)
    }
  }
}
